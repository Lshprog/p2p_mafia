package roles

import (
	"fmt"
	"mafia-p2p/config"
	"mafia-p2p/game"
)

// Mafia kills one non-Mafia player each night.
type Mafia struct{ NodeID int }

func (m *Mafia) Name() string            { return "Mafia" }
func (m *Mafia) HasNightAction() bool    { return true }
func (m *Mafia) NightActionName() string { return "KILL" }

func (m *Mafia) ValidateNightAction(targetID int, state *game.GameState) bool {
	if targetID == m.NodeID {
		return false
	}
	p, ok := state.Players[targetID]
	if !ok || !p.IsAlive {
		return false
	}
	// Cannot kill another Mafia member
	return p.Role != config.RoleMafia
}

func (m *Mafia) PrivateInfo(allRoles map[int]Role) string {
	allies := []int{}
	for id, r := range allRoles {
		if r.Name() == "Mafia" && id != m.NodeID {
			allies = append(allies, id)
		}
	}
	if len(allies) == 0 {
		return "You are the Mafia — the sole member. Eliminate Civilians each night."
	}
	return fmt.Sprintf("You are the Mafia. Your allies: %v. Eliminate Civilians each night.", allies)
}
