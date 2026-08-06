/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package activationpool contains the refill controller for ActivationPools.
// It maintains warm capacity-pod count between warm.min and warm.max by
// creating pods from the pool's PodTemplate and deleting excess idle ones. It
// never observes claims directly (the Activate hot path is write-free); instead
// the sandbox marks a pod state=claimed on bind, which the controller sees as a
// warm deficit and refills. Bursts of deficit within spec.activation.coalescing
// collapse into one refill via a delayed workqueue enqueue.
package activationpool

import (
	"context"
	"fmt"
	"sort"
	"time"

	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	activationinformers "k8s.io/client-go/informers/activation/v1alpha1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	activationlisters "k8s.io/client-go/listers/activation/v1alpha1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
)

// DefaultCoalescingWindow batches a burst of warm-deficit events into a single
// refill when a pool does not set spec.activation.coalescing.
const DefaultCoalescingWindow = 50 * time.Millisecond

// podTemplateKind is the only templateRef kind supported in Phase 0.
const podTemplateKind = "PodTemplate"

var poolControllerKind = activationv1alpha1.SchemeGroupVersion.WithKind("ActivationPool")

// Controller reconciles ActivationPools toward their warm floor.
type Controller struct {
	kubeClient clientset.Interface

	poolLister  activationlisters.ActivationPoolLister
	poolsSynced cache.InformerSynced

	podLister  corelisters.PodLister
	podsSynced cache.InformerSynced

	templateLister  corelisters.PodTemplateLister
	templatesSynced cache.InformerSynced

	// expectations damps create/delete storms while the informer catches up, so
	// a burst of syncs for one pool does not overshoot warm.max.
	expectations *controller.ControllerExpectations

	queue workqueue.TypedRateLimitingInterface[string]
}

// NewController wires the pool/pod/podtemplate informers into a refill controller.
func NewController(
	ctx context.Context,
	kubeClient clientset.Interface,
	poolInformer activationinformers.ActivationPoolInformer,
	podInformer coreinformers.PodInformer,
	templateInformer coreinformers.PodTemplateInformer,
) (*Controller, error) {
	logger := klog.FromContext(ctx)
	c := &Controller{
		kubeClient:      kubeClient,
		poolLister:      poolInformer.Lister(),
		poolsSynced:     poolInformer.Informer().HasSynced,
		podLister:       podInformer.Lister(),
		podsSynced:      podInformer.Informer().HasSynced,
		templateLister:  templateInformer.Lister(),
		templatesSynced: templateInformer.Informer().HasSynced,
		expectations:    controller.NewControllerExpectations(),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "activationpool"},
		),
	}

	if _, err := poolInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.enqueuePool(obj) },
		UpdateFunc: func(_, newObj any) { c.enqueuePool(newObj) },
		DeleteFunc: func(obj any) {
			if pool, ok := poolFromDeleteObj(obj); ok {
				c.expectations.DeleteExpectations(logger, poolKey(pool.Namespace, pool.Name))
			}
		},
	}); err != nil {
		return nil, err
	}

	if _, err := podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addPod,
		UpdateFunc: func(_, newObj any) { c.updatePod(newObj) },
		DeleteFunc: c.deletePod,
	}); err != nil {
		return nil, err
	}

	return c, nil
}

// Run starts the workers and blocks until ctx is cancelled.
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting activationpool controller")
	defer logger.Info("Shutting down activationpool controller")

	if !cache.WaitForNamedCacheSync("activationpool", ctx.Done(), c.poolsSynced, c.podsSynced, c.templatesSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.UntilWithContext(ctx, c.worker, time.Second)
	}
	<-ctx.Done()
}

func (c *Controller) worker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	if err := c.sync(ctx, key); err != nil {
		utilruntime.HandleError(fmt.Errorf("syncing activationpool %q: %w", key, err))
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

func (c *Controller) enqueuePool(obj any) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	c.queue.Add(key)
}

// enqueuePoolAfter schedules a pool sync after the pool's coalescing window.
// Repeated enqueues for the same key within the window collapse to one sync.
func (c *Controller) enqueuePoolAfter(namespace, name string) {
	c.queue.AddAfter(poolKey(namespace, name), c.coalescingWindow(namespace, name))
}

func (c *Controller) coalescingWindow(namespace, name string) time.Duration {
	pool, err := c.poolLister.ActivationPools(namespace).Get(name)
	if err != nil || pool.Spec.Activation.Coalescing == nil || pool.Spec.Activation.Coalescing.Duration <= 0 {
		return DefaultCoalescingWindow
	}
	return pool.Spec.Activation.Coalescing.Duration
}

func (c *Controller) addPod(obj any) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return
	}
	poolName, ok := poolNameForPod(pod)
	if !ok {
		return
	}
	logger := klog.Background()
	c.expectations.CreationObserved(logger, poolKey(pod.Namespace, poolName))
	c.enqueuePoolAfter(pod.Namespace, poolName)
}

func (c *Controller) updatePod(obj any) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return
	}
	if poolName, ok := poolNameForPod(pod); ok {
		c.enqueuePoolAfter(pod.Namespace, poolName)
	}
}

func (c *Controller) deletePod(obj any) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		pod, ok = tombstone.Obj.(*v1.Pod)
		if !ok {
			return
		}
	}
	poolName, ok := poolNameForPod(pod)
	if !ok {
		return
	}
	c.expectations.DeletionObserved(klog.Background(), poolKey(pod.Namespace, poolName))
	c.enqueuePoolAfter(pod.Namespace, poolName)
}

func (c *Controller) sync(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	// Wait for prior create/delete batches to be observed before acting again.
	if !c.expectations.SatisfiedExpectations(logger, key) {
		return nil
	}

	pool, err := c.poolLister.ActivationPools(namespace).Get(name)
	if apierrors.IsNotFound(err) {
		// Capacity pods are owner-ref'd and garbage-collected with the pool.
		c.expectations.DeleteExpectations(logger, key)
		return nil
	}
	if err != nil {
		return err
	}

	selector := labels.SelectorFromSet(labels.Set{activationv1alpha1.LabelPoolName: name})
	pods, err := c.podLister.Pods(namespace).List(selector)
	if err != nil {
		return err
	}

	plan := planRefill(pool, pods)
	var errs []error
	if plan.create > 0 {
		if err := c.createCapacity(ctx, pool, plan.create, key); err != nil {
			errs = append(errs, err)
		}
	}
	if len(plan.deleteNames) > 0 {
		if err := c.deleteCapacity(ctx, namespace, plan.deleteNames, key); err != nil {
			errs = append(errs, err)
		}
	}
	return utilerrors.NewAggregate(errs)
}

func (c *Controller) createCapacity(ctx context.Context, pool *activationv1alpha1.ActivationPool, count int, key string) error {
	logger := klog.FromContext(ctx)
	if pool.Spec.TemplateRef.Kind != podTemplateKind || (pool.Spec.TemplateRef.APIGroup != nil && *pool.Spec.TemplateRef.APIGroup != "") {
		return fmt.Errorf("templateRef must reference a core PodTemplate, got apiGroup=%v kind=%q", pool.Spec.TemplateRef.APIGroup, pool.Spec.TemplateRef.Kind)
	}
	template, err := c.templateLister.PodTemplates(pool.Namespace).Get(pool.Spec.TemplateRef.Name)
	if err != nil {
		return fmt.Errorf("resolving PodTemplate %q: %w", pool.Spec.TemplateRef.Name, err)
	}

	if err := c.expectations.ExpectCreations(logger, key, count); err != nil {
		return err
	}
	var errs []error
	for i := 0; i < count; i++ {
		pod := buildCapacityPod(pool, template)
		if _, err := c.kubeClient.CoreV1().Pods(pool.Namespace).Create(ctx, pod, metav1.CreateOptions{}); err != nil {
			// Undo the unmet expectation so the next sync can retry promptly.
			c.expectations.CreationObserved(logger, key)
			errs = append(errs, err)
		}
	}
	return utilerrors.NewAggregate(errs)
}

func (c *Controller) deleteCapacity(ctx context.Context, namespace string, names []string, key string) error {
	logger := klog.FromContext(ctx)
	if err := c.expectations.ExpectDeletions(logger, key, len(names)); err != nil {
		return err
	}
	var errs []error
	for _, name := range names {
		err := c.kubeClient.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			c.expectations.DeletionObserved(logger, key)
			errs = append(errs, err)
			continue
		}
		if apierrors.IsNotFound(err) {
			// Already gone; the delete event may never fire, so settle it now.
			c.expectations.DeletionObserved(logger, key)
		}
	}
	return utilerrors.NewAggregate(errs)
}

// refillDecision is the output of the pure planner: how many capacity pods to
// create and which existing capacity pods (by name) to delete.
type refillDecision struct {
	create      int
	deleteNames []string
}

// planRefill computes the create/delete plan for one pool from its current pods.
// It is pure (no clients) so the warm-floor/ceiling math is unit-testable.
//
// Rules:
//   - warm = active (non-terminating) pods not marked claimed.
//   - create toward warm.min, but never push total active over warm.max.
//   - delete only idle (warm) pods that exceed warm.max, and never below warm.min.
//   - claimed pods are never deleted (they back live sessions).
func planRefill(pool *activationv1alpha1.ActivationPool, pods []*v1.Pod) refillDecision {
	floor := int(pool.Spec.Warm.Min)
	ceil := int(pool.Spec.Warm.Max)
	if ceil < floor {
		ceil = floor
	}

	var active, warm []*v1.Pod
	for _, p := range pods {
		if p.DeletionTimestamp != nil {
			continue
		}
		active = append(active, p)
		if !isClaimed(p) {
			warm = append(warm, p)
		}
	}

	create := floor - len(warm)
	if create < 0 {
		create = 0
	}
	if room := ceil - len(active); create > room {
		if room < 0 {
			room = 0
		}
		create = room
	}

	overflow := len(active) - ceil
	deletableWarm := len(warm) - floor
	del := 0
	if overflow > 0 && deletableWarm > 0 {
		del = overflow
		if del > deletableWarm {
			del = deletableWarm
		}
	}

	var victims []string
	if del > 0 {
		// Delete the newest idle capacity pods first: minimizes disruption to
		// longer-lived warm capacity that is more likely to be needed.
		sort.Slice(warm, func(i, j int) bool {
			return warm[i].CreationTimestamp.After(warm[j].CreationTimestamp.Time)
		})
		for i := 0; i < del && i < len(warm); i++ {
			victims = append(victims, warm[i].Name)
		}
	}

	return refillDecision{create: create, deleteNames: victims}
}

func buildCapacityPod(pool *activationv1alpha1.ActivationPool, template *v1.PodTemplate) *v1.Pod {
	spec := template.Template.Spec.DeepCopy()
	if pool.Spec.PriorityClassName != "" {
		spec.PriorityClassName = pool.Spec.PriorityClassName
	}

	podLabels := map[string]string{}
	for k, v := range template.Template.Labels {
		podLabels[k] = v
	}
	podLabels[activationv1alpha1.LabelPoolName] = pool.Name

	annotations := map[string]string{}
	for k, v := range template.Template.Annotations {
		annotations[k] = v
	}
	annotations[activationv1alpha1.AnnotationState] = activationv1alpha1.StateWarm
	// Record the checkpoint this capacity pod is warmed from so the scheduler can
	// prefer nodes that already hold it (checkpoint locality). Empty for Phase 0
	// idle-pod pools with no source.
	if pool.Spec.Source != nil && pool.Spec.Source.CheckpointRef != "" {
		annotations[activationv1alpha1.AnnotationCheckpoint] = pool.Spec.Source.CheckpointRef
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName:    pool.Name + "-",
			Namespace:       pool.Namespace,
			Labels:          podLabels,
			Annotations:     annotations,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(pool, poolControllerKind)},
		},
		Spec: *spec,
	}
}

func poolKey(namespace, name string) string {
	return namespace + "/" + name
}

func poolNameForPod(pod *v1.Pod) (string, bool) {
	if pod.Labels == nil {
		return "", false
	}
	name := pod.Labels[activationv1alpha1.LabelPoolName]
	return name, name != ""
}

func isClaimed(pod *v1.Pod) bool {
	return pod.Annotations != nil && pod.Annotations[activationv1alpha1.AnnotationState] == activationv1alpha1.StateClaimed
}

func poolFromDeleteObj(obj any) (*activationv1alpha1.ActivationPool, bool) {
	if pool, ok := obj.(*activationv1alpha1.ActivationPool); ok {
		return pool, true
	}
	tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
	if !ok {
		return nil, false
	}
	pool, ok := tombstone.Obj.(*activationv1alpha1.ActivationPool)
	return pool, ok
}
