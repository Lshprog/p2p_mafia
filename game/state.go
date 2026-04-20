// game/state.go — GameState and StateMachine.
//
// GameState is derived entirely by replaying the ActionLog.
// It is never stored directly — always recomputed from the log.
// This is the Replicated State Machine guarantee.
package game

import (
	"fmt"
	"log"
	"mafia-p2p/config"
	"sync"
)

// PlayerInfo holds per-player runtime state.
type PlayerInfo struct {
	NodeID  int
	Role    config.Role
	IsAlive bool
}

// GameState is the current snapshot of the game, derived from the log.
type GameState struct {
	Phase       config.Phase
	DayNumber   int
	RoundNumber int

	Players map[int]*PlayerInfo // nodeID → info

	// Pending Day votes: voterID → targetID (reset each Day)
	Votes map[int]int

	// Night actions buffered this Night: each entry is {actor_id, action, target_id}
	NightActions []map[string]any

	// Human-readable announcements for the current phase
	Announcements []string

	Winner string // "Mafia" | "Civilians" | ""
}

// NewGameState initialises state from a role map.
func NewGameState(roleMap [config.NumNodes]config.Role) *GameState {
	players := make(map[int]*PlayerInfo, config.NumNodes)
	for id, role := range roleMap {
		players[id] = &PlayerInfo{NodeID: id, Role: role, IsAlive: true}
	}
	return &GameState{
		Phase:   config.PhaseLobby,
		Players: players,
		Votes:   make(map[int]int),
	}
}

// AlivePlayers returns all living PlayerInfo entries.
func (s *GameState) AlivePlayers() []*PlayerInfo {
	result := make([]*PlayerInfo, 0, len(s.Players))
	for _, p := range s.Players {
		if p.IsAlive {
			result = append(result, p)
		}
	}
	return result
}

// AliveIDs returns the node IDs of all living players.
func (s *GameState) AliveIDs() []int {
	alive := s.AlivePlayers()
	ids := make([]int, len(alive))
	for i, p := range alive {
		ids[i] = p.NodeID
	}
	return ids
}

// CheckWinCondition returns "Mafia", "Civilians", or "" if the game continues.
func (s *GameState) CheckWinCondition() string {
	mafia, civs := 0, 0
	for _, p := range s.Players {
		if !p.IsAlive {
			continue
		}
		if p.Role == config.RoleMafia {
			mafia++
		} else {
			civs++
		}
	}
	if mafia == 0 {
		return "Civilians"
	}
	if mafia >= civs {
		return "Mafia"
	}
	return ""
}

// ── StateMachine ──────────────────────────────────────────────────────────────

// StateMachine replays LogEntry values to build GameState incrementally.
type StateMachine struct {
	State *GameState
	mu    sync.Mutex
}

// NewStateMachine creates a StateMachine seeded with the given role map.
func NewStateMachine(roleMap [config.NumNodes]config.Role) *StateMachine {
	return &StateMachine{State: NewGameState(roleMap)}
}

// Apply advances the state by one committed log entry.
func (sm *StateMachine) Apply(actionType string, actorID int, payload map[string]any) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	s := sm.State
	switch actionType {
	case "PHASE_CHANGE":
		sm.applyPhaseChange(payload)
	case "SPEAK":
		// ephemeral — no persistent state change
	case "VOTE":
		sm.applyVote(actorID, payload)
	case "ELIMINATE":
		sm.applyEliminate(payload)
	case "NIGHT_ACTION":
		sm.applyNightAction(actorID, payload)
	case "NIGHT_RESOLVE":
		sm.applyNightResolve(payload)
	case "GAME_RESET":
		sm.applyGameReset()
	}

	if winner := s.CheckWinCondition(); winner != "" {
		s.Winner = winner
		s.Phase = config.PhaseEnded
	}
}

func (sm *StateMachine) applyPhaseChange(payload map[string]any) {
	s := sm.State
	newPhaseStr, _ := payload["new_phase"].(string)
	newPhase, ok := config.PhaseFromString(newPhaseStr)
	if !ok {
		return
	}
	s.Phase = newPhase
	s.RoundNumber++
	switch newPhase {
	case config.PhaseDay:
		s.DayNumber++
		s.Votes = make(map[int]int)
		s.NightActions = nil
		s.Announcements = nil
	case config.PhaseNight:
		s.NightActions = nil
	}
}

func (sm *StateMachine) applyVote(voterID int, payload map[string]any) {
	if target, ok := payloadInt(payload, "target_id"); ok {
		sm.State.Votes[voterID] = target
	}
}

func (sm *StateMachine) applyEliminate(payload map[string]any) {
	s := sm.State
	if target, ok := payloadInt(payload, "target_id"); ok {
		if p, exists := s.Players[target]; exists {
			p.IsAlive = false
			s.Announcements = append(s.Announcements,
				formatf("Player %d was eliminated by vote.", target))
			// Remove any vote cast BY the eliminated player
			delete(s.Votes, target)
		}
	}
}

func (sm *StateMachine) applyNightAction(actorID int, payload map[string]any) {
	entry := map[string]any{
		"actor_id":  actorID,
		"action":    payload["action"],
		"target_id": payload["target_id"],
	}
	sm.State.NightActions = append(sm.State.NightActions, entry)
}

func (sm *StateMachine) applyNightResolve(payload map[string]any) {
	s := sm.State
	killTarget, hasKill := payloadInt(payload, "kill_target")
	protectTarget, hasProtect := payloadInt(payload, "protect_target")

	if hasKill {
		if hasProtect && killTarget == protectTarget {
			s.Announcements = append(s.Announcements,
				formatf("Player %d was protected and survived the night.", killTarget))
		} else {
			if p, ok := s.Players[killTarget]; ok {
				p.IsAlive = false
				s.Announcements = append(s.Announcements,
					formatf("Player %d was eliminated during the night.", killTarget))
			}
		}
	}
	s.NightActions = nil
}

func (sm *StateMachine) applyGameReset() {
	s := sm.State

	// Reset all players to alive
	for _, p := range s.Players {
		p.IsAlive = true
	}

	// Reset game state
	s.Phase = config.PhaseLobby
	s.DayNumber = 0
	s.RoundNumber = 0
	s.Votes = make(map[int]int)
	s.NightActions = nil
	s.Announcements = nil
	s.Winner = ""

	log.Printf("[StateMachine] Game reset - back to LOBBY")
}

// ── helpers ───────────────────────────────────────────────────────────────────

func payloadInt(m map[string]any, key string) (int, bool) {
	v, ok := m[key]
	if !ok {
		return 0, false
	}
	switch x := v.(type) {
	case int:
		return x, true
	case float64:
		return int(x), true
	case int64:
		return int(x), true
	}
	return 0, false
}

func formatf(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}
