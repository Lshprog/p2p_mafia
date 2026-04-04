// distributed/ricart_agrawala.go — Ricart-Agrawala Distributed Mutual Exclusion.
//
// Protocol for one named critical section (csName):
//
//	REQUEST phase:
//	  1. Tick VC and broadcast RA_REQUEST with timestamp.
//	  2. Wait until RA_REPLY received from every active peer.
//
//	REPLY rules on receiving RA_REQUEST from peer P:
//	  • Not in CS and not waiting → reply immediately.
//	  • Holding CS              → defer reply until exit.
//	  • Waiting for CS (both sent REQUEST):
//	      - Our request has priority (lower VC / lower ID) → defer.
//	      - Their request has priority → reply immediately.
//
//	EXIT: flush all deferred replies.
//
// CSes used in the game:
//
//	"SPEAK"        — Day phase speaking floor
//	"NIGHT_ACTION" — Night phase role action
package distributed

import (
	"log"
	"mafia-p2p/config"
	"mafia-p2p/networking"
	"sync"
	"time"
)

type csState int

const (
	csReleased csState = iota
	csWanted
	csHeld
)

// RicartAgrawala manages one named critical section for one node.
type RicartAgrawala struct {
	nodeID int
	csName string
	vc     *VectorClock
	net    *networking.PeerNetwork

	mu          sync.Mutex
	state       csState
	requestTS   []int      // our own REQUEST timestamp (set when state==csWanted)
	pendingFrom map[int]bool // peers we are still waiting for a REPLY from
	repliedCh   chan struct{} // closed when pendingFrom is empty

	deferred []int // peers whose REPLY we deferred
}

// NewRicartAgrawala creates an RA instance for the given critical section name.
func NewRicartAgrawala(nodeID int, csName string,
	vc *VectorClock, net *networking.PeerNetwork) *RicartAgrawala {
	return &RicartAgrawala{
		nodeID: nodeID,
		csName: csName,
		vc:     vc,
		net:    net,
	}
}

// ── Public API ────────────────────────────────────────────────────────────────

// RequestCS blocks until this node acquires the critical section.
// Returns true on success, false if timed out (all peers dead).
func (ra *RicartAgrawala) RequestCS() bool {
	// Collect currently alive peers
	aliveMap := ra.net.AliveNodes()
	peers := make([]int, 0, len(aliveMap))
	for id, alive := range aliveMap {
		if alive {
			peers = append(peers, id)
		}
	}

	if len(peers) == 0 {
		// Sole node alive — enter immediately
		ra.mu.Lock()
		ra.state = csHeld
		ra.mu.Unlock()
		return true
	}

	ts := ra.vc.Tick()

	repliedCh := make(chan struct{})
	pending := make(map[int]bool, len(peers))
	for _, id := range peers {
		pending[id] = true
	}

	ra.mu.Lock()
	ra.state = csWanted
	ra.requestTS = ts
	ra.pendingFrom = pending
	ra.repliedCh = repliedCh
	ra.mu.Unlock()

	// Broadcast REQUEST
	msg := networking.NewRARequest(ra.nodeID, ts, ra.csName)
	ra.net.Broadcast(msg)
	log.Printf("[RA/%s] Node %d sent REQUEST, waiting for %v", ra.csName, ra.nodeID, peers)

	// Wait for all REPLYs, with timeout-based dead-node removal
	deadline := time.Now().Add(config.RARequestTimeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		select {
		case <-repliedCh:
			goto entered
		case <-time.After(min(remaining, 300*time.Millisecond)):
			// Check if any pending peer is now dead
			alive := ra.net.AliveNodes()
			ra.mu.Lock()
			for id := range ra.pendingFrom {
				if !alive[id] {
					delete(ra.pendingFrom, id)
				}
			}
			if len(ra.pendingFrom) == 0 {
				ra.mu.Unlock()
				goto entered
			}
			ra.mu.Unlock()
		}
	}

entered:
	ra.mu.Lock()
	ra.state = csHeld
	ra.mu.Unlock()
	log.Printf("[RA/%s] Node %d ENTERED CS", ra.csName, ra.nodeID)
	return true
}

// ReleaseCS exits the critical section and flushes deferred replies.
func (ra *RicartAgrawala) ReleaseCS() {
	ra.mu.Lock()
	ra.state = csReleased
	ra.requestTS = nil
	deferred := ra.deferred
	ra.deferred = nil
	ra.mu.Unlock()

	for _, peerID := range deferred {
		ts := ra.vc.Tick()
		reply := networking.NewRAReply(ra.nodeID, ts, peerID, ra.csName)
		ra.net.Send(peerID, reply)
		log.Printf("[RA/%s] Node %d sent deferred REPLY to %d", ra.csName, ra.nodeID, peerID)
	}
}

// NotifyNodeDead removes a dead node from pending replies.
// Called by PlayerNode when PeerNetwork declares a node dead.
func (ra *RicartAgrawala) NotifyNodeDead(deadID int) {
	ra.mu.Lock()
	defer ra.mu.Unlock()
	if ra.state != csWanted {
		return
	}
	delete(ra.pendingFrom, deadID)
	if len(ra.pendingFrom) == 0 && ra.repliedCh != nil {
		select {
		case <-ra.repliedCh: // already closed
		default:
			close(ra.repliedCh)
		}
	}
}

// ── Inbound message handlers ──────────────────────────────────────────────────

// OnRARequest handles an incoming RA_REQUEST from a peer.
func (ra *RicartAgrawala) OnRARequest(msg networking.Message) {
	if s, _ := networking.PayloadStr(msg.Payload, "cs_name"); s != ra.csName {
		return
	}
	ra.vc.Update(msg.VectorTS)
	requesterID := msg.SenderID
	requesterTS := msg.VectorTS

	ra.mu.Lock()
	state := ra.state
	myTS := ra.requestTS
	ra.mu.Unlock()

	shouldDefer := false
	if state == csHeld {
		shouldDefer = true
	} else if state == csWanted && myTS != nil {
		// Defer only if WE have priority (requester must wait for us)
		ourPriority := HasPriority(ra.nodeID, myTS, requesterID, requesterTS)
		shouldDefer = ourPriority
	}

	if shouldDefer {
		ra.mu.Lock()
		ra.deferred = append(ra.deferred, requesterID)
		ra.mu.Unlock()
		log.Printf("[RA/%s] Node %d deferred REPLY to %d", ra.csName, ra.nodeID, requesterID)
	} else {
		ts := ra.vc.Tick()
		reply := networking.NewRAReply(ra.nodeID, ts, requesterID, ra.csName)
		ra.net.Send(requesterID, reply)
	}
}

// OnRAReply handles an incoming RA_REPLY directed at this node.
func (ra *RicartAgrawala) OnRAReply(msg networking.Message) {
	if s, _ := networking.PayloadStr(msg.Payload, "cs_name"); s != ra.csName {
		return
	}
	toNode, _ := networking.PayloadInt(msg.Payload, "to_node")
	if toNode != ra.nodeID {
		return
	}
	ra.vc.Update(msg.VectorTS)

	ra.mu.Lock()
	defer ra.mu.Unlock()
	delete(ra.pendingFrom, msg.SenderID)
	log.Printf("[RA/%s] Node %d got REPLY from %d, still waiting: %v",
		ra.csName, ra.nodeID, msg.SenderID, ra.pendingFrom)
	if len(ra.pendingFrom) == 0 && ra.repliedCh != nil {
		select {
		case <-ra.repliedCh:
		default:
			close(ra.repliedCh)
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
