// distributed/paxos.go — Simplified Single-Decree Paxos for the Action Log.
//
// Each Paxos instance decides one log slot. Slots are processed sequentially.
//
// Roles (not fixed — any node can temporarily be Proposer):
//
//	Proposer  → drives Phase 1 (PREPARE/PROMISE) and Phase 2 (ACCEPT/ACCEPTED)
//	Acceptor  → every node acts as Acceptor at all times
//	Learner   → every node learns via PAXOS_COMMIT broadcast
//
// Ballot numbers: encoded as  seq*10 + nodeID  so ballots from different
// nodes are always distinguishable and naturally ordered.
//
// Quorum is a per-call parameter, not a global constant.
//
//   - Regular game actions (SPEAK, VOTE, NIGHT_ACTION, ELIMINATE, NIGHT_RESOLVE)
//     use a majority quorum: config.QuorumSize (N/2+1 = 3).
//     This tolerates up to 2 node failures.
//
//   - PHASE_CHANGE uses an "all-alive" quorum: the number of nodes currently
//     connected at the moment the proposal is made.
//     A phase must not flip until every living participant has acted.
//     If a node dies mid-round its slot in the quorum is released automatically
//     via the same dead-node notification path used by Ricart-Agrawala.
package distributed

import (
	"encoding/json"
	"log"
	"mafia-p2p/networking"
	"sync"
	"time"
)

// CommitCallback is called when a log slot is committed.
type CommitCallback func(LogEntry)

// ── Per-slot acceptor state ───────────────────────────────────────────────────

type acceptorState struct {
	minProposal    int
	acceptedBallot *int
	acceptedValue  map[string]any
}

// ── Paxos engine ──────────────────────────────────────────────────────────────

// Paxos handles both Proposer and Acceptor roles for one node.
type Paxos struct {
	nodeID int
	log    *ActionLog
	vc     *VectorClock
	net    *networking.PeerNetwork

	// Acceptor state per slot
	accMu    sync.Mutex
	acceptor map[int]*acceptorState

	// Proposer in-flight state per slot (only when we are proposing)
	propMu   sync.Mutex
	proposer map[int]*proposerState

	// Highest ballot seen (for generating higher ballots)
	ballotMu  sync.Mutex
	maxBallot int

	callbacks []CommitCallback
	cbMu      sync.RWMutex
}

type proposerState struct {
	ballot   int
	phase    string // "prepare" | "accept"
	quorum   int    // responses needed to proceed (varies per action type)
	promises []*promisePayload
	accepted int
	mu       sync.Mutex
	done     chan struct{}
}

type promisePayload struct {
	AcceptedBallot *int
	AcceptedValue  map[string]any
}

// NewPaxos creates a Paxos engine for the given node.
func NewPaxos(nodeID int, log *ActionLog, vc *VectorClock,
	net *networking.PeerNetwork) *Paxos {
	return &Paxos{
		nodeID:   nodeID,
		log:      log,
		vc:       vc,
		net:      net,
		acceptor: make(map[int]*acceptorState),
		proposer: make(map[int]*proposerState),
	}
}

// RegisterCallback registers a function to be called on each commit.
func (p *Paxos) RegisterCallback(cb CommitCallback) {
	p.cbMu.Lock()
	defer p.cbMu.Unlock()
	p.callbacks = append(p.callbacks, cb)
}

// ── Public: propose ───────────────────────────────────────────────────────────

// Propose runs Paxos as Proposer to commit value to the next available log slot.
//
// quorum is the number of PROMISE/ACCEPTED responses required to proceed.
// Pass config.QuorumSize for regular actions (majority fault-tolerant quorum).
// Pass the current alive-node count for PHASE_CHANGE (all-alive barrier).
//
// Blocks until committed or max retries exceeded.
// Returns true if a value was committed (may be a different value if contention).
func (p *Paxos) Propose(payload map[string]any, actionType string, quorum int) bool {
	const maxRetries = 5
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*100) * time.Millisecond)
		}
		slot := p.log.NextSlot()
		ballot := p.newBallot()
		log.Printf("[Paxos] Node %d proposing slot=%d ballot=%d quorum=%d (attempt %d)",
			p.nodeID, slot, ballot, quorum, attempt+1)

		// Phase 1
		promises := p.runPrepare(slot, ballot, quorum)
		if promises == nil {
			log.Printf("[Paxos] Phase 1 failed slot=%d", slot)
			continue
		}

		// If any promise carries a prior accepted value, adopt the highest one
		adoptedValue := payload
		highestAB := -1
		for _, pr := range promises {
			if pr.AcceptedBallot != nil && *pr.AcceptedBallot > highestAB {
				highestAB = *pr.AcceptedBallot
				adoptedValue = pr.AcceptedValue
			}
		}

		// Phase 2
		if !p.runAccept(slot, ballot, adoptedValue, quorum) {
			log.Printf("[Paxos] Phase 2 failed slot=%d", slot)
			continue
		}

		// Commit
		ts := p.vc.Tick()
		entry := NewLogEntry(slot, actionType, p.nodeID, adoptedValue, ts)
		p.commitEntry(entry)
		entryMap := logEntryToMap(entry)
		commitMsg := networking.NewPaxosCommit(p.nodeID, ts, entryMap)
		p.net.Broadcast(commitMsg)
		return true
	}
	return false
}

// ── Phase 1 ───────────────────────────────────────────────────────────────────

func (p *Paxos) runPrepare(slot, ballot, quorum int) []*promisePayload {
	done := make(chan struct{})
	ps := &proposerState{ballot: ballot, phase: "prepare", quorum: quorum, done: done}

	p.propMu.Lock()
	p.proposer[slot] = ps
	p.propMu.Unlock()

	defer func() {
		p.propMu.Lock()
		delete(p.proposer, slot)
		p.propMu.Unlock()
	}()

	// Self-promise (counts toward quorum)
	if pr := p.acceptorHandlePrepare(slot, ballot); pr != nil {
		ps.mu.Lock()
		ps.promises = append(ps.promises, pr)
		if len(ps.promises) >= quorum {
			select {
			case <-done:
			default:
				close(done)
			}
		}
		ps.mu.Unlock()
	}

	// Broadcast PREPARE
	ts := p.vc.Tick()
	msg := networking.NewPaxosPrepare(p.nodeID, ts, slot, ballot)
	p.net.Broadcast(msg)

	select {
	case <-done:
	case <-time.After(config.PaxosPrepareTimeout):
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	if len(ps.promises) >= quorum {
		return ps.promises
	}
	return nil
}

// ── Phase 2 ───────────────────────────────────────────────────────────────────

func (p *Paxos) runAccept(slot, ballot int, value map[string]any, quorum int) bool {
	done := make(chan struct{})
	ps := &proposerState{ballot: ballot, phase: "accept", quorum: quorum, done: done}

	p.propMu.Lock()
	p.proposer[slot] = ps
	p.propMu.Unlock()

	defer func() {
		p.propMu.Lock()
		delete(p.proposer, slot)
		p.propMu.Unlock()
	}()

	// Self-accept (counts toward quorum)
	if p.acceptorHandleAccept(slot, ballot, value) {
		ps.mu.Lock()
		ps.accepted++
		if ps.accepted >= quorum {
			select {
			case <-done:
			default:
				close(done)
			}
		}
		ps.mu.Unlock()
	}

	ts := p.vc.Tick()
	msg := networking.NewPaxosAccept(p.nodeID, ts, slot, ballot, value)
	p.net.Broadcast(msg)

	select {
	case <-done:
	case <-time.After(config.PaxosAcceptTimeout):
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	return ps.accepted >= quorum
}

// ── Acceptor logic ────────────────────────────────────────────────────────────

func (p *Paxos) getAcceptor(slot int) *acceptorState {
	p.accMu.Lock()
	defer p.accMu.Unlock()
	if _, ok := p.acceptor[slot]; !ok {
		p.acceptor[slot] = &acceptorState{minProposal: -1}
	}
	return p.acceptor[slot]
}

func (p *Paxos) acceptorHandlePrepare(slot, ballot int) *promisePayload {
	acc := p.getAcceptor(slot)
	p.accMu.Lock()
	defer p.accMu.Unlock()
	if ballot > acc.minProposal {
		acc.minProposal = ballot
		p.updateMaxBallot(ballot)
		return &promisePayload{
			AcceptedBallot: acc.acceptedBallot,
			AcceptedValue:  acc.acceptedValue,
		}
	}
	return nil
}

func (p *Paxos) acceptorHandleAccept(slot, ballot int, value map[string]any) bool {
	acc := p.getAcceptor(slot)
	p.accMu.Lock()
	defer p.accMu.Unlock()
	if ballot >= acc.minProposal {
		acc.minProposal = ballot
		b := ballot
		acc.acceptedBallot = &b
		acc.acceptedValue = value
		p.updateMaxBallot(ballot)
		return true
	}
	return false
}

func (p *Paxos) newBallot() int {
	p.ballotMu.Lock()
	defer p.ballotMu.Unlock()
	seq := p.maxBallot/10 + 1
	p.maxBallot = seq*10 + p.nodeID
	return p.maxBallot
}

func (p *Paxos) updateMaxBallot(b int) {
	p.ballotMu.Lock()
	defer p.ballotMu.Unlock()
	if b > p.maxBallot {
		p.maxBallot = b
	}
}

// ── Inbound handlers ──────────────────────────────────────────────────────────

// OnPrepare handles an incoming PAXOS_PREPARE message.
func (p *Paxos) OnPrepare(msg networking.Message) {
	p.vc.Update(msg.VectorTS)
	slot, _ := networking.PayloadInt(msg.Payload, "slot")
	ballot, _ := networking.PayloadInt(msg.Payload, "ballot")
	p.updateMaxBallot(ballot)

	pr := p.acceptorHandlePrepare(slot, ballot)
	ts := p.vc.Tick()

	var ab *int
	var av map[string]any
	if pr != nil {
		ab = pr.AcceptedBallot
		av = pr.AcceptedValue
		p.net.Send(msg.SenderID, networking.NewPaxosPromise(p.nodeID, ts, slot, ballot, ab, av))
	} else {
		// NACK: send Promise with ballot=-1
		neg := -1
		p.net.Send(msg.SenderID, networking.NewPaxosPromise(p.nodeID, ts, slot, -1, &neg, nil))
	}
}

// OnPromise handles an incoming PAXOS_PROMISE.
func (p *Paxos) OnPromise(msg networking.Message) {
	p.vc.Update(msg.VectorTS)
	slot, _ := networking.PayloadInt(msg.Payload, "slot")
	ballot, _ := networking.PayloadInt(msg.Payload, "ballot")
	if ballot < 0 {
		return // NACK
	}

	p.propMu.Lock()
	ps := p.proposer[slot]
	p.propMu.Unlock()
	if ps == nil || ps.phase != "prepare" || ps.ballot != ballot {
		return
	}

	// Decode accepted_ballot and accepted_value
	var ab *int
	if v, ok := msg.Payload["accepted_ballot"]; ok && v != nil {
		if f, ok2 := v.(float64); ok2 {
			i := int(f)
			ab = &i
		}
	}
	var av map[string]any
	if v, ok := msg.Payload["accepted_value"]; ok && v != nil {
		if m, ok2 := v.(map[string]any); ok2 {
			av = m
		}
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.promises = append(ps.promises, &promisePayload{AcceptedBallot: ab, AcceptedValue: av})
	if len(ps.promises) >= ps.quorum {
		select {
		case <-ps.done:
		default:
			close(ps.done)
		}
	}
}

// OnAccept handles an incoming PAXOS_ACCEPT.
func (p *Paxos) OnAccept(msg networking.Message) {
	p.vc.Update(msg.VectorTS)
	slot, _ := networking.PayloadInt(msg.Payload, "slot")
	ballot, _ := networking.PayloadInt(msg.Payload, "ballot")

	var value map[string]any
	if v, ok := msg.Payload["value"]; ok {
		if m, ok2 := v.(map[string]any); ok2 {
			value = m
		}
	}

	if p.acceptorHandleAccept(slot, ballot, value) {
		ts := p.vc.Tick()
		p.net.Send(msg.SenderID, networking.NewPaxosAccepted(p.nodeID, ts, slot, ballot))
	}
}

// OnAccepted handles an incoming PAXOS_ACCEPTED.
func (p *Paxos) OnAccepted(msg networking.Message) {
	p.vc.Update(msg.VectorTS)
	slot, _ := networking.PayloadInt(msg.Payload, "slot")
	ballot, _ := networking.PayloadInt(msg.Payload, "ballot")

	p.propMu.Lock()
	ps := p.proposer[slot]
	p.propMu.Unlock()
	if ps == nil || ps.phase != "accept" || ps.ballot != ballot {
		return
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.accepted++
	if ps.accepted >= ps.quorum {
		select {
		case <-ps.done:
		default:
			close(ps.done)
		}
	}
}

// OnCommit handles an incoming PAXOS_COMMIT.
func (p *Paxos) OnCommit(msg networking.Message) {
	p.vc.Update(msg.VectorTS)
	entryRaw, ok := msg.Payload["entry"]
	if !ok {
		return
	}
	// Re-marshal and unmarshal via JSON to convert map[string]any → LogEntry
	data, err := json.Marshal(entryRaw)
	if err != nil {
		return
	}
	var entry LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return
	}
	p.commitEntry(entry)
}

// ── Commit ────────────────────────────────────────────────────────────────────

func (p *Paxos) commitEntry(entry LogEntry) {
	p.log.CommitOrFill(entry)
	log.Printf("[Paxos] Committed slot %d: %s by node %d",
		entry.Slot, entry.ActionType, entry.ActorID)
	p.cbMu.RLock()
	cbs := p.callbacks
	p.cbMu.RUnlock()
	for _, cb := range cbs {
		cb(entry)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// logEntryToMap converts a LogEntry to map[string]any for JSON embedding.
func logEntryToMap(e LogEntry) map[string]any {
	return map[string]any{
		"slot":        e.Slot,
		"action_type": e.ActionType,
		"actor_id":    e.ActorID,
		"payload":     e.Payload,
		"vector_ts":   e.VectorTS,
		"wall_time":   e.WallTime,
	}
}
