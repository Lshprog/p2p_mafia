// node/roles/role.go — Role interface and factory.
package roles

import (
	"mafia-p2p/config"
	"mafia-p2p/game"
)

// Role defines the behaviour of a player's secret role.
type Role interface {
	// Name returns the display name of the role.
	Name() string

	// HasNightAction returns true if this role acts during the Night phase.
	HasNightAction() bool

	// NightActionName returns the action keyword ("KILL", "PROTECT") or "".
	NightActionName() string

	// ValidateNightAction returns true if targeting targetID is legal.
	ValidateNightAction(targetID int, state *game.GameState) bool

	// PrivateInfo returns a string that is shown only to the local player.
	PrivateInfo(allRoles map[int]Role) string
}

// New constructs the correct Role implementation for the given config.Role.
func New(nodeID int, r config.Role) Role {
	switch r {
	case config.RoleMafia:
		return &Mafia{NodeID: nodeID}
	case config.RoleDoctor:
		return &Doctor{NodeID: nodeID}
	default:
		return &Civilian{NodeID: nodeID}
	}
}

// NewAll constructs a role map for all nodes.
func NewAll(roleMap [config.NumNodes]config.Role) map[int]Role {
	result := make(map[int]Role, config.NumNodes)
	for id, r := range roleMap {
		result[id] = New(id, r)
	}
	return result
}
