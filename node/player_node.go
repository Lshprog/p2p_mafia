// node/player_node.go — The core PlayerNode.
//
// A PlayerNode is the single top-level object that each player runs.
// It wires together:
//
//	PeerNetwork       — TCP messaging
//	VectorClock       — causal ordering
//	ActionLog         — append-only history
//	StateMachine      — derives game state from log
//	RicartAgrawala×2  — SPEAK and NIGHT_ACTION critical sections
//	Paxos             — consensus for log commits
//	Role              — role-specific behaviour
package node

import (
	"fmt"
	"log"
	"mafia-p2p/config"
	"mafia-p2p/distributed"
	"mafia-p2p/game"
	"mafia-p2p/networking"
	"mafia-p2p/node/roles"
	"sync"
	"time"
)

// PlayerNode is the main object for one player in the distributed game.
type PlayerNode struct {
	NodeID int

	// Distributed primitives
	VC      *distributed.VectorClock
	Log     *distributed.ActionLog
	Network *networking.PeerNetwork
	CsSpeak *distributed.RicartAgrawala
	CsNight *distributed.RicartAgrawala
	Paxos   *distributed.Paxos

	// Game logic
	Role roles.Role
	SM   *game.StateMachine

	// Optional callbacks (set by the UI layer)
	OnStateChange func(*game.GameState)           // called after each log commit
	OnMessage     func(senderID int, text string) // called on SPEAK messages

	// ── Phase-change participation tracking ──────────────────────────────────
	// A PHASE_CHANGE may only be proposed once every alive node has acted.
	// These sets are reset whenever a PHASE_CHANGE is committed.
	//
	// For DAY → NIGHT:  every alive node must have committed a SPEAK or VOTE.
	// For NIGHT → DAY:  every alive node that has a night action (Mafia, Doctor)
	//                   must have committed a NIGHT_ACTION.
	//
	// Both sets are keyed by node ID and updated from the Paxos commit callback,
	// so every node maintains identical tracking from the same shared log.
	participationMu sync.RWMutex
	spokeOrVoted    map[int]bool // Day participation
	nightActed      map[int]bool // Night participation

	// ── Game automation ──────────────────────────────────────────────────────
	coordinatorStopCh chan struct{}

	started bool
}

// NewPlayerNode constructs a fully wired PlayerNode for the given node ID.
func NewPlayerNode(nodeID int, roleMap [config.NumNodes]config.Role) *PlayerNode {
	vc := distributed.NewVectorClock(nodeID)
	al := distributed.NewActionLog()
	net := networking.NewPeerNetwork(nodeID)

	csSpeak := distributed.NewRicartAgrawala(nodeID, "SPEAK", vc, net)
	csNight := distributed.NewRicartAgrawala(nodeID, "NIGHT_ACTION", vc, net)
	paxos := distributed.NewPaxos(nodeID, al, vc, net)

	myRole := roles.New(nodeID, roleMap[nodeID])
	allRoles := roles.NewAll(roleMap)
	log.Printf("[Node %d] %s", nodeID, myRole.PrivateInfo(allRoles))

	sm := game.NewStateMachine(roleMap)

	pn := &PlayerNode{
		NodeID:            nodeID,
		VC:                vc,
		Log:               al,
		Network:           net,
		CsSpeak:           csSpeak,
		CsNight:           csNight,
		Paxos:             paxos,
		Role:              myRole,
		SM:                sm,
		spokeOrVoted:      make(map[int]bool),
		nightActed:        make(map[int]bool),
		coordinatorStopCh: make(chan struct{}),
	}

	// Register the Paxos commit callback — single source of truth for all state updates.
	paxos.RegisterCallback(func(entry distributed.LogEntry) {
		sm.Apply(entry.ActionType, entry.ActorID, entry.Payload)
		pn.trackParticipation(entry)
		if pn.OnStateChange != nil {
			pn.OnStateChange(sm.State)
		}
	})

	return pn
}

// Start boots all background goroutines and registers message handlers.
func (pn *PlayerNode) Start() error {
	if pn.started {
		return nil
	}
	pn.started = true

	// Inject VC snapshot into heartbeat sender
	pn.Network.SetVCCallback(pn.VC.Snapshot)

	// Notify RA instances when a peer dies
	pn.Network.SetNodeDeadCallback(func(deadID int) {
		pn.CsSpeak.NotifyNodeDead(deadID)
		pn.CsNight.NotifyNodeDead(deadID)
	})

	// ── Register all message handlers ────────────────────────────────────────

	// RA handlers
	pn.Network.RegisterHandler(networking.MsgRARequest, pn.CsSpeak.OnRARequest)
	pn.Network.RegisterHandler(networking.MsgRARequest, pn.CsNight.OnRARequest)
	pn.Network.RegisterHandler(networking.MsgRAReply, pn.CsSpeak.OnRAReply)
	pn.Network.RegisterHandler(networking.MsgRAReply, pn.CsNight.OnRAReply)

	// Paxos handlers
	pn.Network.RegisterHandler(networking.MsgPaxosPrepare, pn.Paxos.OnPrepare)
	pn.Network.RegisterHandler(networking.MsgPaxosPromise, pn.Paxos.OnPromise)
	pn.Network.RegisterHandler(networking.MsgPaxosAccept, pn.Paxos.OnAccept)
	pn.Network.RegisterHandler(networking.MsgPaxosAccepted, pn.Paxos.OnAccepted)
	pn.Network.RegisterHandler(networking.MsgPaxosCommit, pn.Paxos.OnCommit)

	// System handlers
	pn.Network.RegisterHandler(networking.MsgHeartbeat, pn.onHeartbeat)
	pn.Network.RegisterHandler(networking.MsgSpeak, pn.onSpeakMsg)
	pn.Network.RegisterHandler(networking.MsgSyncRequest, pn.onSyncRequest)
	pn.Network.RegisterHandler(networking.MsgSyncResponse, pn.onSyncResponse)

	// Start the game coordinator goroutine
	go pn.coordinatorLoop()

	if err := pn.Network.Start(); err != nil {
		return err
	}

	// After network starts, request sync from peers to catch up
	// (in case we're rejoining after a disconnect)
	go func() {
		time.Sleep(2 * time.Second) // Wait for connections to establish
		if pn.Log.Len() == 0 {
			// Empty log means we might be behind, request sync
			log.Printf("[Node %d] Empty log on startup, requesting sync", pn.NodeID)
			pn.RequestSync()
		}
	}()

	return nil
}

// Stop shuts down all connections and goroutines.
func (pn *PlayerNode) Stop() {
	close(pn.coordinatorStopCh)
	pn.Network.Stop()
	log.Printf("[Node %d] stopped", pn.NodeID)
}

// ── Game state accessors ──────────────────────────────────────────────────────

func (pn *PlayerNode) State() *game.GameState { return pn.SM.State }
func (pn *PlayerNode) Phase() config.Phase    { return pn.SM.State.Phase }

// ── Day phase actions ─────────────────────────────────────────────────────────

// Speak acquires the SPEAK critical section and commits the message to the log.
func (pn *PlayerNode) Speak(text string) bool {
	if pn.Phase() != config.PhaseDay {
		log.Printf("[Node %d] Speak() outside Day phase", pn.NodeID)
		return false
	}
	p, ok := pn.State().Players[pn.NodeID]
	if !ok || !p.IsAlive {
		log.Printf("[Node %d] dead players cannot speak", pn.NodeID)
		return false
	}

	log.Printf("[Node %d] Requesting SPEAK CS...", pn.NodeID)
	pn.CsSpeak.RequestCS()
	defer pn.CsSpeak.ReleaseCS()

	// Broadcast real-time chat (ephemeral)
	ts := pn.VC.Tick()
	chatMsg := networking.NewSpeak(pn.NodeID, ts, text)
	pn.Network.Broadcast(chatMsg)

	// Commit to log via Paxos (majority quorum — fault-tolerant)
	return pn.Paxos.Propose(map[string]any{"text": text}, "SPEAK", config.QuorumSize)
}

// Vote casts an elimination vote during the Day phase.
func (pn *PlayerNode) Vote(targetID int) bool {
	if pn.Phase() != config.PhaseDay {
		return false
	}
	if p, ok := pn.State().Players[targetID]; !ok || !p.IsAlive {
		return false
	}
	log.Printf("[Node %d] Voting to eliminate %d", pn.NodeID, targetID)
	return pn.Paxos.Propose(map[string]any{"target_id": targetID}, "VOTE", config.QuorumSize)
}

// ── Night phase actions ───────────────────────────────────────────────────────

// PerformNightAction executes this node's night role action.
// Returns false immediately for roles without a night action (Civilian).
func (pn *PlayerNode) PerformNightAction(targetID int) bool {
	if pn.Phase() != config.PhaseNight {
		return false
	}
	if !pn.Role.HasNightAction() {
		return false
	}
	if !pn.Role.ValidateNightAction(targetID, pn.State()) {
		log.Printf("[Node %d] invalid night action target %d", pn.NodeID, targetID)
		return false
	}
	action := pn.Role.NightActionName()
	log.Printf("[Node %d] Night action: %s → %d", pn.NodeID, action, targetID)

	pn.CsNight.RequestCS()
	defer pn.CsNight.ReleaseCS()

	return pn.Paxos.Propose(
		map[string]any{"action": action, "target_id": targetID},
		"NIGHT_ACTION",
		config.QuorumSize,
	)
}

// ── Phase transitions ─────────────────────────────────────────────────────────

// ProposePhaseChange proposes a phase transition, but only if every alive node
// has participated in the current phase.
//
// DAY → NIGHT  requires every alive node to have committed a SPEAK or VOTE.
// NIGHT → DAY  requires every alive node with a night action (Mafia, Doctor)
//
//	to have committed a NIGHT_ACTION.
//
// If the barrier is not yet satisfied the call returns false immediately —
// the coordinator should retry after each new log commit.
//
// Quorum is set to the number of currently alive nodes (all-alive barrier),
// not the majority. A dead node is automatically excluded from both the
// participation check and the quorum once the failure detector marks it dead.
func (pn *PlayerNode) ProposePhaseChange(newPhase config.Phase) bool {
	// ── Barrier check ─────────────────────────────────────────────────────────
	aliveMap := pn.Network.AliveNodes()
	// AliveNodes returns peer IDs only; add self
	aliveSet := make(map[int]bool, len(aliveMap)+1)
	for id, alive := range aliveMap {
		if alive {
			aliveSet[id] = true
		}
	}
	aliveSet[pn.NodeID] = true
	aliveCount := len(aliveSet)

	pn.participationMu.RLock()
	switch pn.SM.State.Phase {
	case config.PhaseDay:
		// Transition DAY → NIGHT: every alive node must have spoken or voted
		for id := range aliveSet {
			if !pn.spokeOrVoted[id] {
				pn.participationMu.RUnlock()
				log.Printf("[Node %d] Phase change to NIGHT blocked: node %d has not spoken or voted",
					pn.NodeID, id)
				return false
			}
		}
	case config.PhaseNight:
		// Transition NIGHT → DAY: every alive node with a night action must have acted
		for id := range aliveSet {
			if pn.nodeHasNightAction(id) && !pn.nightActed[id] {
				pn.participationMu.RUnlock()
				log.Printf("[Node %d] Phase change to DAY blocked: node %d has not submitted night action",
					pn.NodeID, id)
				return false
			}
		}
	}
	pn.participationMu.RUnlock()

	// ── All-alive Paxos quorum ────────────────────────────────────────────────
	// Every alive node must accept the phase change, not just a majority.
	log.Printf("[Node %d] All %d alive nodes have participated — proposing phase change → %s",
		pn.NodeID, aliveCount, newPhase)
	return pn.Paxos.Propose(
		map[string]any{"new_phase": newPhase.String()},
		"PHASE_CHANGE",
		aliveCount, // all-alive quorum
	)
}

// nodeHasNightAction returns true if the given node's role requires a night action.
// This is determined from the static role map baked into the StateMachine.
func (pn *PlayerNode) nodeHasNightAction(nodeID int) bool {
	p, ok := pn.SM.State.Players[nodeID]
	if !ok {
		return false
	}
	return p.Role == config.RoleMafia || p.Role == config.RoleDoctor
}

// ProposeElimination commits an ELIMINATE entry after a successful vote tally.
func (pn *PlayerNode) ProposeElimination(targetID int) bool {
	return pn.Paxos.Propose(map[string]any{"target_id": targetID}, "ELIMINATE", config.QuorumSize)
}

// ProposeNightResolve commits a NIGHT_RESOLVE entry.
func (pn *PlayerNode) ProposeNightResolve(killTarget, protectTarget *int) bool {
	payload := map[string]any{}
	if killTarget != nil {
		payload["kill_target"] = *killTarget
	}
	if protectTarget != nil {
		payload["protect_target"] = *protectTarget
	}
	return pn.Paxos.Propose(payload, "NIGHT_RESOLVE", config.QuorumSize)
}

// ── Participation tracking ────────────────────────────────────────────────────

// trackParticipation updates the Day/Night participation maps from a committed
// log entry. Called from the Paxos commit callback (single goroutine context
// from each node's perspective, but guarded by participationMu for safety).
func (pn *PlayerNode) trackParticipation(entry distributed.LogEntry) {
	pn.participationMu.Lock()
	defer pn.participationMu.Unlock()

	switch entry.ActionType {
	case "SPEAK", "VOTE":
		// Any alive node that speaks or votes is counted as Day-participating
		pn.spokeOrVoted[entry.ActorID] = true

	case "NIGHT_ACTION":
		// The Mafia or Doctor has submitted their night action
		pn.nightActed[entry.ActorID] = true

	case "PHASE_CHANGE":
		// Reset both sets whenever any phase change commits —
		// the new phase starts with a clean participation slate.
		pn.spokeOrVoted = make(map[int]bool)
		pn.nightActed = make(map[int]bool)

	case "GAME_RESET":
		// Reset participation tracking for new game
		pn.spokeOrVoted = make(map[int]bool)
		pn.nightActed = make(map[int]bool)
	}
}

// ── Inbound handlers ──────────────────────────────────────────────────────────

func (pn *PlayerNode) onHeartbeat(msg networking.Message) {
	pn.VC.Update(msg.VectorTS)
}

func (pn *PlayerNode) onSpeakMsg(msg networking.Message) {
	pn.VC.Update(msg.VectorTS)
	if pn.OnMessage != nil {
		text, _ := networking.PayloadStr(msg.Payload, "text")
		pn.OnMessage(msg.SenderID, text)
	}
}

func (pn *PlayerNode) onSyncRequest(msg networking.Message) {
	pn.VC.Update(msg.VectorTS)
	fromSlot, _ := networking.PayloadInt(msg.Payload, "from_slot")
	entries := pn.Log.EntriesSince(fromSlot)

	// Convert []LogEntry → []any for JSON embedding
	raw := make([]any, len(entries))
	for i, e := range entries {
		raw[i] = map[string]any{
			"slot":        e.Slot,
			"action_type": e.ActionType,
			"actor_id":    e.ActorID,
			"payload":     e.Payload,
			"vector_ts":   e.VectorTS,
			"wall_time":   e.WallTime,
		}
	}
	ts := pn.VC.Tick()
	reply := networking.NewSyncResponse(pn.NodeID, ts, raw)
	pn.Network.Send(msg.SenderID, reply)
}

func (pn *PlayerNode) onSyncResponse(msg networking.Message) {
	pn.VC.Update(msg.VectorTS)
	entriesRaw, _ := msg.Payload["entries"].([]any)
	for _, raw := range entriesRaw {
		m, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		slot, _ := networking.PayloadInt(m, "slot")
		actionType, _ := m["action_type"].(string)
		actorID, _ := networking.PayloadInt(m, "actor_id")
		payload, _ := m["payload"].(map[string]any)
		// vector_ts
		var vts []int
		if arr, ok := m["vector_ts"].([]any); ok {
			for _, v := range arr {
				if f, ok2 := v.(float64); ok2 {
					vts = append(vts, int(f))
				}
			}
		}
		wallTime, _ := m["wall_time"].(float64)
		entry := distributed.LogEntry{
			Slot:       slot,
			ActionType: actionType,
			ActorID:    actorID,
			Payload:    payload,
			VectorTS:   vts,
			WallTime:   int64(wallTime),
		}
		pn.Log.CommitOrFill(entry)
		pn.SM.Apply(entry.ActionType, entry.ActorID, entry.Payload)
	}
}

// RequestSync broadcasts a sync request to catch up after reconnection.
func (pn *PlayerNode) RequestSync() {
	fromSlot := pn.Log.NextSlot()
	ts := pn.VC.Tick()
	msg := networking.NewSyncRequest(pn.NodeID, ts, fromSlot)
	pn.Network.Broadcast(msg)
	log.Printf("[Node %d] Sync requested from slot %d", pn.NodeID, fromSlot)
}

// ── Game Coordinator & Automation ─────────────────────────────────────────────

// coordinatorLoop runs in background and orchestrates the game flow.
// Only the lowest-ID alive node acts as coordinator to avoid duplicate proposals.
func (pn *PlayerNode) coordinatorLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-pn.coordinatorStopCh:
			return
		case <-ticker.C:
			if !pn.shouldCoordinate() {
				continue
			}
			pn.checkAndAdvanceGame()
		}
	}
}

// shouldCoordinate returns true if this node should act as coordinator.
// The lowest-ID alive node coordinates to avoid duplicate proposals.
func (pn *PlayerNode) shouldCoordinate() bool {
	alive := pn.State().AliveIDs()
	if len(alive) == 0 {
		return false
	}
	// Sort to find lowest ID
	minID := alive[0]
	for _, id := range alive {
		if id < minID {
			minID = id
		}
	}
	return minID == pn.NodeID
}

// checkAndAdvanceGame checks current phase and advances the game if conditions are met.
func (pn *PlayerNode) checkAndAdvanceGame() {
	s := pn.State()

	// Check win condition first
	if s.Winner != "" {
		// Game has ended - wait 20 seconds then reset
		lastSlot := pn.Log.NextSlot() - 1
		if lastSlot >= 0 {
			if entry, ok := pn.Log.Get(lastSlot); ok {
				// Check if we already proposed a reset
				if entry.ActionType == "GAME_RESET" {
					return // Reset already in progress
				}

				// Check if enough time has passed since game end
				timeSinceEnd := time.Since(time.Unix(0, entry.WallTime))
				if timeSinceEnd > 20*time.Second {
					log.Printf("[Coordinator] Game ended %v ago - resetting game", timeSinceEnd.Round(time.Second))
					pn.ProposeGameReset()
				}
			}
		}
		return
	}

	switch s.Phase {
	case config.PhaseLobby:
		// Auto-start the game after all nodes connect
		pn.autoStartGame()

	case config.PhaseDay:
		// Check if we should tally votes and eliminate
		pn.autoTallyAndEliminate()

	case config.PhaseNight:
		// Check if we should resolve night actions
		pn.autoResolveNight()
	}
}

// autoStartGame transitions from LOBBY to DAY once all nodes are connected.
func (pn *PlayerNode) autoStartGame() {
	// Only start if we're in LOBBY phase
	if pn.State().Phase != config.PhaseLobby {
		return
	}

	// Check both alive status and connection count
	aliveMap := pn.Network.AliveNodes()

	// Count alive peers
	aliveCount := 1 // self
	for _, isAlive := range aliveMap {
		if isAlive {
			aliveCount++
		}
	}

	// Check peer connection count (more reliable early on)
	connectedPeers := pn.Network.ConnectedPeerCount()

	log.Printf("[Coordinator Debug] Alive: %d/%d, Connected peers: %d/%d",
		aliveCount, config.NumNodes, connectedPeers, config.NumNodes-1)

	// Check if we should start
	shouldStart := false

	// Initial start: all peers connected and log is empty
	if connectedPeers >= config.NumNodes-1 && pn.Log.Len() == 0 {
		shouldStart = true
	}

	// After reset: all peers connected and last action was GAME_RESET
	lastSlot := pn.Log.NextSlot() - 1
	if lastSlot >= 0 && connectedPeers >= config.NumNodes-1 {
		if entry, ok := pn.Log.Get(lastSlot); ok {
			if entry.ActionType == "GAME_RESET" {
				shouldStart = true
			}
		}
	}

	if shouldStart {
		log.Printf("[Coordinator] All %d nodes connected - starting game", config.NumNodes)

		// Wait a bit for heartbeats to propagate so all nodes are marked alive
		time.Sleep(3 * time.Second)

		// Use a fixed quorum of NumNodes for initial phase change
		// (not dynamic alive count which might be incomplete due to heartbeat delay)
		payload := map[string]any{"new_phase": config.PhaseDay.String()}
		pn.Paxos.Propose(payload, "PHASE_CHANGE", config.NumNodes)
	}
}

// autoTallyAndEliminate checks if all alive players have voted, then tallies and eliminates.
func (pn *PlayerNode) autoTallyAndEliminate() {
	s := pn.State()
	alive := s.AliveIDs()

	// Wait for all alive players to vote
	if len(s.Votes) < len(alive) {
		return // Not everyone has voted yet
	}

	// Check if an elimination has already been processed
	// (by checking if the last log entry is ELIMINATE)
	lastSlot := pn.Log.NextSlot() - 1
	if lastSlot >= 0 {
		if entry, ok := pn.Log.Get(lastSlot); ok {
			if entry.ActionType == "ELIMINATE" {
				// Already eliminated, now transition to night
				pn.ProposePhaseChange(config.PhaseNight)
				return
			}
		}
	}

	// Tally votes
	tally := make(map[int]int)
	for _, target := range s.Votes {
		tally[target]++
	}

	// Find player with most votes
	maxVotes := 0
	eliminateTarget := -1
	for id, count := range tally {
		if count > maxVotes {
			maxVotes = count
			eliminateTarget = id
		}
	}

	if eliminateTarget >= 0 {
		log.Printf("[Coordinator] Vote tally: %v - eliminating player %d with %d votes",
			tally, eliminateTarget, maxVotes)
		pn.ProposeElimination(eliminateTarget)
	}
}

// autoResolveNight checks if all night-action players have acted, then resolves.
func (pn *PlayerNode) autoResolveNight() {
	s := pn.State()

	// Check if all Mafia and Doctor players have acted
	pn.participationMu.RLock()
	allActed := true
	for _, p := range s.Players {
		if !p.IsAlive {
			continue
		}
		if p.Role == config.RoleMafia || p.Role == config.RoleDoctor {
			if !pn.nightActed[p.NodeID] {
				allActed = false
				break
			}
		}
	}
	pn.participationMu.RUnlock()

	if !allActed {
		return // Wait for everyone
	}

	// Check if resolution already happened
	lastSlot := pn.Log.NextSlot() - 1
	if lastSlot >= 0 {
		if entry, ok := pn.Log.Get(lastSlot); ok {
			if entry.ActionType == "NIGHT_RESOLVE" {
				// Already resolved, transition to day
				pn.ProposePhaseChange(config.PhaseDay)
				return
			}
		}
	}

	// Extract kill and protect targets from night actions
	var killTarget, protectTarget *int
	for _, action := range s.NightActions {
		actionType, _ := action["action"].(string)
		targetID, ok := networking.PayloadInt(action, "target_id")
		if !ok {
			continue
		}

		if actionType == "KILL" {
			killTarget = &targetID
		} else if actionType == "PROTECT" {
			protectTarget = &targetID
		}
	}

	log.Printf("[Coordinator] Night resolution: kill=%v protect=%v", killTarget, protectTarget)
	pn.ProposeNightResolve(killTarget, protectTarget)
}

// ProposeGameReset resets the game back to LOBBY for a new round.
func (pn *PlayerNode) ProposeGameReset() bool {
	log.Printf("[Node %d] Proposing game reset", pn.NodeID)
	return pn.Paxos.Propose(map[string]any{"reset": true}, "GAME_RESET", config.QuorumSize)
}

// DumpState returns a human-readable status string.
func (pn *PlayerNode) DumpState() string {
	s := pn.State()
	return fmt.Sprintf(
		"=== Node %d (%s) ===\nPhase:     %s\nDay:       %d\nLog slots: %d\nAlive:     %v\nVotes:     %v\nWinner:    %s",
		pn.NodeID, pn.Role.Name(),
		s.Phase,
		s.DayNumber,
		pn.Log.Len(),
		s.AliveIDs(),
		s.Votes,
		func() string {
			if s.Winner == "" {
				return "TBD"
			}
			return s.Winner
		}(),
	)
}
