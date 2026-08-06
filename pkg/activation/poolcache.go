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

package activation

import (
	"strconv"
	"sync"
	"time"

	activationv1alpha1 "k8s.io/api/activation/v1alpha1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// capacitySlot is a ready-set entry for one warm sandbox pod: a unit of warm
// capacity projected from the pod (identity + endpoint), not the pod itself.
type capacitySlot struct {
	uid       types.UID
	namespace string
	name      string
	poolKey   string
	podIP     string
	port      int32
}

// claim records an in-memory Activate hold. Abandoned claims return to warm
// after TTL; Activate never writes claim state to the apiserver.
type claim struct {
	slot       capacitySlot
	generation uint64
	expiresAt  time.Time
}

// poolCache is the write-free ready-set + claim ledger. Informer updates must
// not re-admit a UID that is currently claimed (resync cannot clobber a claim).
// This is intentionally not AssumeCache: claims are not optimistic API writes.
type poolCache struct {
	mu sync.Mutex
	// ready[poolKey][uid] = capacitySlot
	ready map[string]map[types.UID]capacitySlot
	// claimed[uid] = claim (overlays informer state)
	claimed map[types.UID]claim
	// pools[poolKey] = latest ActivationPool spec snapshot
	pools map[string]*activationv1alpha1.ActivationPool
	// generations[uid] is the last minted bind generation for that sandbox.
	generations map[types.UID]uint64
	// lastPod keeps the last observed pod for reclaim-after-TTL.
	lastPod map[types.UID]*v1.Pod

	claimTTL time.Duration
	// readyCh is closed and replaced whenever the ready-set may have grown.
	readyCh chan struct{}
}

func newPoolCache(claimTTL time.Duration) *poolCache {
	if claimTTL <= 0 {
		claimTTL = DefaultClaimTTL
	}
	return &poolCache{
		ready:       make(map[string]map[types.UID]capacitySlot),
		claimed:     make(map[types.UID]claim),
		pools:       make(map[string]*activationv1alpha1.ActivationPool),
		generations: make(map[types.UID]uint64),
		lastPod:     make(map[types.UID]*v1.Pod),
		claimTTL:    claimTTL,
		readyCh:     make(chan struct{}),
	}
}

func poolKey(namespace, name string) string {
	return namespace + "/" + name
}

func (c *poolCache) upsertPool(pool *activationv1alpha1.ActivationPool) {
	if pool == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Store a shallow copy of the pointer; callers must not mutate after upsert.
	c.pools[poolKey(pool.Namespace, pool.Name)] = pool
}

func (c *poolCache) deletePool(namespace, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pools, poolKey(namespace, name))
}

func (c *poolCache) getPool(key string) *activationv1alpha1.ActivationPool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pools[key]
}

func (c *poolCache) warmCount(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.ready[key])
}

// observePod updates the ready-set from an informer event. Claimed UIDs are
// never re-added to ready, even if the apiserver still shows state=warm.
func (c *poolCache) observePod(pod *v1.Pod) {
	if pod == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastPod[pod.UID] = pod.DeepCopy()

	key, ok := poolKeyForPod(pod)
	if !ok {
		c.removeReadyLocked(pod.UID)
		return
	}
	// Reconstruct the fencing generation from the sandbox-written annotation.
	// This runs for every capacity pod (warm or claimed) so a server restart
	// re-seeds counters during the initial list-sync and never re-mints a
	// generation a live sandbox already enforces.
	c.reconcileGenerationLocked(pod)
	if _, claimed := c.claimed[pod.UID]; claimed {
		return
	}
	if !isWarmReady(pod) {
		c.removeReadyLocked(pod.UID)
		return
	}
	port := c.portForPoolLocked(key)
	slot := capacitySlot{
		uid:       pod.UID,
		namespace: pod.Namespace,
		name:      pod.Name,
		poolKey:   key,
		podIP:     pod.Status.PodIP,
		port:      port,
	}
	if c.ready[key] == nil {
		c.ready[key] = make(map[types.UID]capacitySlot)
	}
	c.ready[key][pod.UID] = slot
	c.broadcastReadyLocked()
}

func (c *poolCache) deletePod(pod *v1.Pod) {
	if pod == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.lastPod, pod.UID)
	c.removeReadyLocked(pod.UID)
	delete(c.claimed, pod.UID)
	delete(c.generations, pod.UID)
}

// reconcileGenerationLocked advances the in-memory generation for a capacity pod
// to at least the value the sandbox has persisted. Monotonic: an older or absent
// annotation never lowers a counter we have already minted past.
func (c *poolCache) reconcileGenerationLocked(pod *v1.Pod) {
	gen, ok := parseBindGeneration(pod)
	if !ok {
		return
	}
	if gen > c.generations[pod.UID] {
		c.generations[pod.UID] = gen
	}
}

// parseBindGeneration reads the sandbox-written bind-generation annotation.
// A missing or malformed value is treated as absent (no generation to seed).
func parseBindGeneration(pod *v1.Pod) (uint64, bool) {
	if pod.Annotations == nil {
		return 0, false
	}
	raw, ok := pod.Annotations[AnnotationBindGeneration]
	if !ok || raw == "" {
		return 0, false
	}
	gen, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return gen, true
}

func (c *poolCache) removeReadyLocked(uid types.UID) {
	for key, m := range c.ready {
		delete(m, uid)
		if len(m) == 0 {
			delete(c.ready, key)
		}
	}
}

func (c *poolCache) portForPoolLocked(key string) int32 {
	if p := c.pools[key]; p != nil && p.Spec.EndpointPort > 0 {
		return p.Spec.EndpointPort
	}
	return DefaultEndpointPort
}

func (c *poolCache) broadcastReadyLocked() {
	close(c.readyCh)
	c.readyCh = make(chan struct{})
}

func (c *poolCache) waitCh() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readyCh
}

// tryClaim moves up to want capacity slots from ready → claimed. Returns the claims
// on full success; on shortfall returns nil and leaves the ready-set unchanged.
func (c *poolCache) tryClaim(poolKey string, want int, now time.Time) []claim {
	if want <= 0 {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	ready := c.ready[poolKey]
	if len(ready) < want {
		return nil
	}
	out := make([]claim, 0, want)
	picked := make([]types.UID, 0, want)
	for uid, slot := range ready {
		picked = append(picked, uid)
		gen := c.generations[uid] + 1
		c.generations[uid] = gen
		out = append(out, claim{
			slot:       slot,
			generation: gen,
			expiresAt:  now.Add(c.claimTTL),
		})
		if len(out) == want {
			break
		}
	}
	for i, uid := range picked {
		delete(ready, uid)
		c.claimed[uid] = out[i]
	}
	if len(ready) == 0 {
		delete(c.ready, poolKey)
	}
	return out
}

// releaseExpired returns claimed capacity slots whose TTL elapsed to the ready-set
// when the last observed pod is still warm+ready.
func (c *poolCache) releaseExpired(now time.Time) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	released := 0
	for uid, cl := range c.claimed {
		if now.Before(cl.expiresAt) {
			continue
		}
		delete(c.claimed, uid)
		pod := c.lastPod[uid]
		if pod != nil && isWarmReady(pod) {
			key := cl.slot.poolKey
			if c.ready[key] == nil {
				c.ready[key] = make(map[types.UID]capacitySlot)
			}
			cl.slot.port = c.portForPoolLocked(key)
			c.ready[key][uid] = cl.slot
			released++
			c.broadcastReadyLocked()
		}
	}
	return released
}

func poolKeyForPod(pod *v1.Pod) (string, bool) {
	if pod.Labels == nil {
		return "", false
	}
	name := pod.Labels[LabelPoolName]
	if name == "" {
		return "", false
	}
	return poolKey(pod.Namespace, name), true
}

func isWarmReady(pod *v1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	if pod.Status.Phase != v1.PodRunning || pod.Status.PodIP == "" {
		return false
	}
	if pod.Annotations != nil {
		switch pod.Annotations[AnnotationState] {
		case "", StateWarm:
			// eligible
		case StateClaimed:
			return false
		default:
			return false
		}
	}
	return true
}
