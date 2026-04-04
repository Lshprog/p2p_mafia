# Implementation Roadmap — Decentralized Mafia P2P (Go)

## Quick start

```bash
# Build all packages
cd mafia-p2p
go build ./...

# Run a local 5-node game (5 terminals)
go run . 0    # Mafia
go run . 1    # Civilian
go run . 2    # Civilian
go run . 3    # Civilian
go run . 4    # Doctor
```

Requires **Go 1.21+**. Zero external dependencies — pure stdlib.

---

## What is already scaffolded

| Package / File | Status | Description |
|---|---|---|
| `config/config.go` | ✅ | Ports, roles, phases, all timeout constants |
| `distributed/vector_clock.go` | ✅ | Thread-safe VectorClock — Tick / Update / Snapshot / priority comparison |
| `distributed/action_log.go` | ✅ | Append-only `LogEntry` store with gap detection and catch-up slicing |
| `networking/message.go` | ✅ | All message types, 4-byte length-prefix JSON framing, factory helpers |
| `networking/peer_network.go` | ✅ | TCP server + auto-reconnecting clients, heartbeats, failure detector |
| `distributed/ricart_agrawala.go` | ✅ | Full RA — REQUEST/REPLY/defer, VC tie-breaking, dead-node removal |
| `distributed/paxos.go` | ✅ | Single-decree Paxos with retry, quorum logic, multi-slot sequencing |
| `game/state.go` | ✅ | `GameState` + `StateMachine` — replays log, win-condition checks |
| `node/roles/` | ✅ | `Role` interface + `Civilian`, `Mafia`, `Doctor` implementations |
| `node/player_node.go` | ✅ | `PlayerNode` wiring everything: Speak, Vote, NightAction, PhaseChange, Sync |
| `main.go` | ✅ | CLI entry point with interactive command loop |

---

## Phase 1 — Correctness & Integration (Week 1–2)

### 1.1 Vote tallying coordinator
**New file:** `game/coordinator.go`

After enough VOTE entries appear in the log (majority for one target), a node must:
- Scan the log for VOTE entries in the current Day round.
- When ≥ 3 votes target the same player, the **alive node with the lowest ID** calls `ProposeElimination(targetID)`.
- Immediately afterwards, propose `PHASE_CHANGE → NIGHT`.
- Guard with a flag so only one ELIMINATE is proposed per Day.

```go
// game/coordinator.go
func (c *Coordinator) CheckVotes(state *GameState, pn *node.PlayerNode) { ... }
```

### 1.2 Night resolution coordinator
**File:** `game/coordinator.go`

After all role-having nodes have committed their NIGHT_ACTION (or timed out):
- Collect KILL and PROTECT entries for the current Night round from the log.
- The alive node with the lowest ID calls `ProposeNightResolve(killTarget, protectTarget)`.
- Immediately propose `PHASE_CHANGE → DAY`.

### 1.3 Lobby handshake
**New file:** `game/lobby.go`

Before the first DAY phase:
- Add `MsgType = "READY"` to `networking/message.go`.
- Each node broadcasts READY on startup.
- When a node has seen READY from all 4 peers, the lowest-ID node proposes `PHASE_CHANGE → DAY`.

### 1.4 Fix `game/state.go` formatf dependency
The `formatf` helper in `game/state.go` already imports `fmt` — verify `go build ./...` is clean.

---

## Phase 2 — Fault Tolerance (Week 2–3)

### 2.1 RA deadlock guard on node death
**File:** `distributed/ricart_agrawala.go`

`NotifyNodeDead(deadID)` is already scaffolded. Verify that:
- It removes the dead node from `pendingFrom`.
- It closes `repliedCh` when `pendingFrom` becomes empty.
- Write a unit test that kills a node mid-CS-request and verifies the remaining nodes still enter the CS.

### 2.2 Paxos liveness watcher
**File:** `distributed/paxos.go`

If the Proposer crashes after broadcasting ACCEPT but before COMMIT:
- Add `watchSlot(slot int)` — a goroutine that fires after `PaxosPrepareTimeout` if the slot is still uncommitted.
- On timeout, the watcher starts a new Propose round with a higher ballot.

```go
func (p *Paxos) watchSlot(slot int) {
    time.Sleep(config.PaxosPrepareTimeout * 2)
    if _, ok := p.log.Get(slot); !ok {
        p.Propose(lastKnownValue, lastKnownType) // re-propose
    }
}
```

### 2.3 State reconciliation on reconnection
**File:** `node/player_node.go`

`RequestSync()` is already scaffolded. Complete the reconnection flow:
- When `PeerNetwork` detects a previously-dead peer sending a heartbeat, call `RequestSync()` automatically.
- The `onSyncResponse` handler already applies received entries — verify ordering is correct.
- Add a `SYNC_ACK` message so the recovering node knows sync is complete before it resumes gameplay.

---

## Phase 3 — Testing (Week 3)

### 3.1 Unit tests
**Directory:** `tests/` (or per-package `_test.go` files — idiomatic Go)

| Test file | What to verify |
|---|---|
| `distributed/vector_clock_test.go` | Tick, Update, HappensBefore, Concurrent, HasPriority |
| `distributed/action_log_test.go` | Commit, gap error, CommitOrFill, EntriesSince |
| `distributed/ricart_agrawala_test.go` | 2-node concurrent REQUEST — exactly one enters CS; deferred replies flushed on exit |
| `distributed/paxos_test.go` | Single proposer commits; two concurrent proposers converge; retry on NACK |
| `game/state_test.go` | Replay a hand-crafted sequence of Apply() calls and assert correct GameState |

Run: `go test ./...`

### 3.2 Integration simulation
**File:** `node/player_node_test.go`

Spin up all 5 `PlayerNode` instances in-process (different port sets to avoid collision):
1. All nodes propose `PHASE_CHANGE → DAY`.
2. Nodes 1–4 each vote to eliminate node 0 (Mafia).
3. Coordinator detects majority → ProposeElimination(0) → ProposePhaseChange(NIGHT).
4. Assert `state.Winner == "Civilians"` on all nodes.

---

## Phase 4 — User Interface (Week 4)

### 4.1 Terminal UI with `tcell` or `bubbletea`
**New file:** `ui/tui.go`

Replace the `bufio.Scanner` loop in `main.go` with a proper TUI:
- Top panel: game log / announcements.
- Middle panel: live chat (SPEAK messages).
- Bottom panel: command input.

Recommended library: [Bubble Tea](https://github.com/charmbracelet/bubbletea) (`go get github.com/charmbracelet/bubbletea`).

Wire `pn.OnStateChange` and `pn.OnMessage` to update panels via the Bubble Tea `Update()` model.

### 4.2 Role-specific night prompts
During Night phase display only what is relevant to the local role:

```
Mafia:    "Choose a player to ELIMINATE: [1 2 3 4]"
Doctor:   "Choose a player to PROTECT:   [0 1 2 3 4]"
Civilian: "Night phase — waiting for morning…  (no action)"
```

---

## Phase 5 — Polish (Week 4–5)

- [ ] Shuffle role assignment at game start (use `math/rand` with a shared seed negotiated via Paxos).
- [ ] Auto-advance phases on timeout (`config.DayDiscussionTimeout` / `config.NightActionTimeout`).
- [ ] Write `docs/architecture.md` explaining each algorithm choice (VC, RA, Paxos).
- [ ] Record a demo: 5 terminals, one full game.
- [ ] Run `go vet ./...` and `staticcheck ./...`; fix all warnings.

---

## Files to touch in recommended order

```
Week 1   game/coordinator.go          [NEW]  vote tally + night resolve
         game/lobby.go                [NEW]  ready handshake
         networking/message.go               add MsgReady type

Week 2   distributed/paxos.go                watchSlot liveness
         distributed/ricart_agrawala.go       verify NotifyNodeDead
         node/player_node.go                  auto-sync on reconnect

Week 3   distributed/*_test.go        [NEW]  unit tests
         node/player_node_test.go     [NEW]  integration test

Week 4   ui/tui.go                    [NEW]  Bubble Tea TUI
         main.go                             wire TUI

Week 5   config/config.go                    shuffle + timers
         docs/architecture.md         [NEW]
```
