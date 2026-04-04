package roles

import "mafia-p2p/game"

// Doctor protects one player each night (including themselves).
type Doctor struct{ NodeID int }

func (d *Doctor) Name() string            { return "Doctor" }
func (d *Doctor) HasNightAction() bool    { return true }
func (d *Doctor) NightActionName() string { return "PROTECT" }

func (d *Doctor) ValidateNightAction(targetID int, state *game.GameState) bool {
	p, ok := state.Players[targetID]
	return ok && p.IsAlive
}

func (d *Doctor) PrivateInfo(_ map[int]Role) string {
	return "You are the Doctor. Each night choose one player to protect. " +
		"If the Mafia targets them, they survive."
}
