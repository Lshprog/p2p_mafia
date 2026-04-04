// distributed/action_log.go — Append-Only Action Log (RSM backbone).
//
// The log is the single source of truth. Game state is always derived by
// replaying the log — entries are never deleted or mutated.
//
// Each entry is written only after Paxos quorum agreement.
package distributed

import (
	"fmt"
	"sync"
	"time"
)

// LogEntry is one committed, immutable record in the action log.
type LogEntry struct {
	Slot       int            `json:"slot"`        // monotonically increasing Paxos slot
	ActionType string         `json:"action_type"` // "VOTE","KILL","PROTECT","SPEAK","PHASE_CHANGE",…
	ActorID    int            `json:"actor_id"`    // originating node
	Payload    map[string]any `json:"payload"`     // action-specific data
	VectorTS   []int          `json:"vector_ts"`   // Vector Clock snapshot at commit time
	WallTime   int64          `json:"wall_time"`   // Unix nano (informational)
}

// NewLogEntry constructs an entry with the current wall-clock time.
func NewLogEntry(slot int, actionType string, actorID int,
	payload map[string]any, vectorTS []int) LogEntry {
	return LogEntry{
		Slot:       slot,
		ActionType: actionType,
		ActorID:    actorID,
		Payload:    payload,
		VectorTS:   vectorTS,
		WallTime:   time.Now().UnixNano(),
	}
}

// ActionLog is a thread-safe, append-only slice of LogEntry values.
type ActionLog struct {
	mu      sync.RWMutex
	entries []LogEntry
}

// NewActionLog returns an empty ActionLog.
func NewActionLog() *ActionLog {
	return &ActionLog{}
}

// Commit appends entry to the log.
// Returns an error if entry.Slot is not the next expected slot (gap detection).
func (l *ActionLog) Commit(entry LogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	expected := len(l.entries)
	if entry.Slot != expected {
		return fmt.Errorf("log gap: expected slot %d, got %d", expected, entry.Slot)
	}
	l.entries = append(l.entries, entry)
	return nil
}

// CommitOrFill silently skips duplicate slots and applies entries in order.
// Use this for recovery catch-up replay.
func (l *ActionLog) CommitOrFill(entry LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if entry.Slot < len(l.entries) {
		return // already committed
	}
	if entry.Slot == len(l.entries) {
		l.entries = append(l.entries, entry)
	}
	// out-of-order entries are ignored; caller must send in order
}

// Get returns the entry at the given slot, or false if not yet committed.
func (l *ActionLog) Get(slot int) (LogEntry, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if slot < 0 || slot >= len(l.entries) {
		return LogEntry{}, false
	}
	return l.entries[slot], true
}

// All returns a copy of all committed entries.
func (l *ActionLog) All() []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := make([]LogEntry, len(l.entries))
	copy(result, l.entries)
	return result
}

// NextSlot returns the slot index that the next Commit call must use.
func (l *ActionLog) NextSlot() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}

// EntriesSince returns all entries from slot onwards (for catch-up sync).
func (l *ActionLog) EntriesSince(slot int) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if slot >= len(l.entries) {
		return nil
	}
	result := make([]LogEntry, len(l.entries)-slot)
	copy(result, l.entries[slot:])
	return result
}

// Len returns the number of committed slots.
func (l *ActionLog) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries)
}
