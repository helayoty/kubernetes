/*
Copyright 2025 The Kubernetes Authors.

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

package workloadmanager

import (
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"
)

// defaultSchedulingTimeoutSeconds defines how long the gang pods should wait at the
// Permit stage for a quorum before being rejected.
const defaultSchedulingTimeoutSeconds = 300

// podGroupKey uniquely identifies a specific instance of a PodGroup.
type podGroupKey struct {
	namespace    string
	workloadName string
	podGroupName string
	replicaKey string
}

func newPodGroupKey(namespace string, workloadRef *v1.WorkloadReference) podGroupKey {
	return podGroupKey{
		namespace:    namespace,
		workloadName: workloadRef.Name,
		podGroupName: workloadRef.PodGroup,
		replicaKey: workloadRef.PodGroupReplicaKey,
	}
}

// podGroupInfo holds the runtime state of a pod group.
type podGroupInfo struct {
	lock sync.RWMutex
	// allPods tracks all pods belonging to the group that are known to the scheduler.
	allPods sets.Set[types.UID]
	// assumedPods tracks pods that have reached the Reserve stage and are waiting
	// for the rest of the gang to arrive before being allowed to bind.
	assumedPods sets.Set[types.UID]
	// scheduledPods tracks all pods belonging to the group that are scheduled.
	scheduledPods sets.Set[types.UID]
	// schedulingDeadline stores the time at which the gang will time out.
	// It is initialized when the first pod from the group enters the Permit stage.
	schedulingDeadline *time.Time
}

func newPodGroupInfo() *podGroupInfo {
	return &podGroupInfo{
		allPods:       sets.New[types.UID](),
		assumedPods:   sets.New[types.UID](),
		scheduledPods: sets.New[types.UID](),
	}
}

// AllPods returns the UIDs of all pods known to the scheduler for this group.
func (pgs *podGroupInfo) AllPods() sets.Set[types.UID] {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.allPods.Clone()
}

// AssumedPods returns the UIDs of all pods for this group in the assumed state,
// i.e., passed the Reserve gate.
func (pgs *podGroupInfo) AssumedPods() sets.Set[types.UID] {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.assumedPods.Clone()
}

// ScheduledPods returns the UIDs of all pods already scheduled for this group.
func (pgs *podGroupInfo) ScheduledPods() sets.Set[types.UID] {
	pgs.lock.RLock()
	defer pgs.lock.RUnlock()

	return pgs.scheduledPods.Clone()
}

// SchedulingTimeout returns the remaining time until the pod group scheduling times out.
// A new deadline is created if one doesn't exist, or if the previous one has expired.
func (pgs *podGroupInfo) SchedulingTimeout() time.Duration {
	pgs.lock.Lock()
	defer pgs.lock.Unlock()

	// A new deadline is set if one doesn't exist, or if the old one has passed.
	// This allows a new attempt to form a gang after a previous attempt timed out.
	if pgs.schedulingDeadline == nil || pgs.schedulingDeadline.Before(time.Now()) {
		pgs.schedulingDeadline = ptr.To(time.Now().Add(defaultSchedulingTimeoutSeconds * time.Second))
	}
	return time.Until(*pgs.schedulingDeadline)
}

// AssumePod marks a pod as having reached the Reserve stage.
func (pgs *podGroupInfo) AssumePod(podUID types.UID) {
	pgs.lock.Lock()
	defer pgs.lock.Unlock()

	pgs.assumedPods.Insert(podUID)
}

// ForgetPod removes a pod from the assumed state.
func (pgs *podGroupInfo) ForgetPod(podUID types.UID) {
	pgs.lock.Lock()
	defer pgs.lock.Unlock()

	pgs.assumedPods.Delete(podUID)
}
