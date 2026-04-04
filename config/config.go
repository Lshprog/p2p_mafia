// config/config.go — Static configuration for the Decentralized Mafia P2P system.
package config

import "time"

// ── Node registry ────────────────────────────────────────────────────────────

// NodeAddr holds the host:port for one player node.
type NodeAddr struct {
	Host string
	Port int
}

// NumNodes is the fixed number of player nodes.
const NumNodes = 5

// QuorumSize is the minimum votes needed for Paxos consensus (majority).
const QuorumSize = NumNodes/2 + 1 // 3

// NodeRegistry maps node IDs (0-4) to their network addresses.
// In a real deployment replace "localhost" with actual IPs.
var NodeRegistry = [NumNodes]NodeAddr{
	{Host: "localhost", Port: 5000},
	{Host: "localhost", Port: 5001},
	{Host: "localhost", Port: 5002},
	{Host: "localhost", Port: 5003},
	{Host: "localhost", Port: 5004},
}

// ── Roles ────────────────────────────────────────────────────────────────────

type Role int

const (
	RoleMafia    Role = iota
	RoleCivilian Role = iota
	RoleDoctor   Role = iota
)

func (r Role) String() string {
	switch r {
	case RoleMafia:
		return "Mafia"
	case RoleCivilian:
		return "Civilian"
	case RoleDoctor:
		return "Doctor"
	default:
		return "Unknown"
	}
}

// DefaultRoleMap assigns a role to each node ID.
// Shuffle this before game start for a real game.
var DefaultRoleMap = [NumNodes]Role{
	0: RoleMafia,
	1: RoleCivilian,
	2: RoleCivilian,
	3: RoleCivilian,
	4: RoleDoctor,
}

// ── Game phases ───────────────────────────────────────────────────────────────

type Phase int

const (
	PhaseLobby Phase = iota
	PhaseDay   Phase = iota
	PhaseNight Phase = iota
	PhaseEnded Phase = iota
)

func (p Phase) String() string {
	switch p {
	case PhaseLobby:
		return "LOBBY"
	case PhaseDay:
		return "DAY"
	case PhaseNight:
		return "NIGHT"
	case PhaseEnded:
		return "ENDED"
	default:
		return "UNKNOWN"
	}
}

func PhaseFromString(s string) (Phase, bool) {
	switch s {
	case "LOBBY":
		return PhaseLobby, true
	case "DAY":
		return PhaseDay, true
	case "NIGHT":
		return PhaseNight, true
	case "ENDED":
		return PhaseEnded, true
	}
	return PhaseLobby, false
}

// ── Timing ────────────────────────────────────────────────────────────────────

const (
	HeartbeatInterval   = 2 * time.Second
	NodeTimeout         = 6 * time.Second
	RARequestTimeout    = 5 * time.Second
	PaxosPrepareTimeout = 3 * time.Second
	PaxosAcceptTimeout  = 3 * time.Second
	DayDiscussionTimeout = 60 * time.Second
	NightActionTimeout  = 30 * time.Second
)
