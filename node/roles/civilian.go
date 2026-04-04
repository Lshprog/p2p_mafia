package roles

import "mafia-p2p/game"

// Civilian has no night action — it survives by voting correctly during the day.
type Civilian struct{ NodeID int }

func (c *Civilian) Name() string           { return "Civilian" }
func (c *Civilian) HasNightAction() bool   { return false }
func (c *Civilian) NightActionName() string { return "" }

func (c *Civilian) ValidateNightAction(_ int, _ *game.GameState) bool { return false }

func (c *Civilian) PrivateInfo(_ map[int]Role) string {
	return "You are a Civilian. Survive, discuss, and vote out the Mafia during the Day phase."
}
