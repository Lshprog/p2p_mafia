// distributed/vector_clock.go — Thread-safe Vector Clock.
//
// Rules:
//   - Local event / before send → Tick()  (increments own counter)
//   - On receive(msg)           → Update() (element-wise max, then tick)
//
// Comparison helpers power Ricart-Agrawala priority resolution.
package distributed

import (
	"mafia-p2p/config"
	"sync"
)

// VectorClock holds an N-element logical clock for one node.
type VectorClock struct {
	mu     sync.Mutex
	NodeID int
	clock  []int
}

// NewVectorClock creates a zero-initialised VectorClock for nodeID.
func NewVectorClock(nodeID int) *VectorClock {
	return &VectorClock{
		NodeID: nodeID,
		clock:  make([]int, config.NumNodes),
	}
}

// Tick increments this node's own counter (local event or before send).
// Returns a copy of the clock after the increment.
func (vc *VectorClock) Tick() []int {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	vc.clock[vc.NodeID]++
	return copySlice(vc.clock)
}

// Update merges an incoming vector: clock[i] = max(clock[i], incoming[i]),
// then increments own counter. Returns a copy after the merge.
func (vc *VectorClock) Update(incoming []int) []int {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	for i, v := range incoming {
		if i < len(vc.clock) && v > vc.clock[i] {
			vc.clock[i] = v
		}
	}
	vc.clock[vc.NodeID]++
	return copySlice(vc.clock)
}

// Snapshot returns a copy of the current clock without modifying it.
func (vc *VectorClock) Snapshot() []int {
	vc.mu.Lock()
	defer vc.mu.Unlock()
	return copySlice(vc.clock)
}

// ── Comparison helpers ────────────────────────────────────────────────────────

// HappensBefore returns true if v1 causally precedes v2 (v1 → v2).
func HappensBefore(v1, v2 []int) bool {
	less := false
	for i := range v1 {
		if v1[i] > v2[i] {
			return false
		}
		if v1[i] < v2[i] {
			less = true
		}
	}
	return less
}

// Concurrent returns true if neither v1 → v2 nor v2 → v1.
func Concurrent(v1, v2 []int) bool {
	return !HappensBefore(v1, v2) && !HappensBefore(v2, v1)
}

// HasPriority decides Ricart-Agrawala tie-breaking:
// requester wins if its timestamp causally precedes ours,
// or if concurrent but requesterID < localID.
func HasPriority(requesterID int, requesterTS []int, localID int, localTS []int) bool {
	if HappensBefore(requesterTS, localTS) {
		return true
	}
	if Concurrent(requesterTS, localTS) {
		return requesterID < localID
	}
	return false
}

// ── helpers ───────────────────────────────────────────────────────────────────

func copySlice(s []int) []int {
	c := make([]int, len(s))
	copy(c, s)
	return c
}
