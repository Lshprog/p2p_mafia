// networking/message.go — Message envelope and all message type definitions.
//
// Every packet on the wire is a Message serialised to JSON, framed with a
// 4-byte big-endian length prefix.
//
// Protocol families:
//
//	SYSTEM  → heartbeat, sync
//	RA      → Ricart-Agrawala mutual exclusion
//	PAXOS   → Paxos consensus rounds
//	GAME    → speak, vote, night-action, phase-change
package networking

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

// MsgType identifies the purpose of a message.
type MsgType string

const (
	// System
	MsgHeartbeat    MsgType = "HEARTBEAT"
	MsgSyncRequest  MsgType = "SYNC_REQUEST"
	MsgSyncResponse MsgType = "SYNC_RESPONSE"

	// Ricart-Agrawala
	MsgRARequest MsgType = "RA_REQUEST"
	MsgRAReply   MsgType = "RA_REPLY"

	// Paxos
	MsgPaxosPrepare  MsgType = "PAXOS_PREPARE"
	MsgPaxosPromise  MsgType = "PAXOS_PROMISE"
	MsgPaxosAccept   MsgType = "PAXOS_ACCEPT"
	MsgPaxosAccepted MsgType = "PAXOS_ACCEPTED"
	MsgPaxosCommit   MsgType = "PAXOS_COMMIT"

	// Game
	MsgSpeak       MsgType = "SPEAK"
	MsgVote        MsgType = "VOTE"
	MsgNightAction MsgType = "NIGHT_ACTION"
	MsgPhaseChange MsgType = "PHASE_CHANGE"
)

// Message is the universal wire envelope.
type Message struct {
	MsgType   MsgType        `json:"msg_type"`
	SenderID  int            `json:"sender_id"`
	VectorTS  []int          `json:"vector_ts"`
	Payload   map[string]any `json:"payload"`
	MsgID     string         `json:"msg_id"`
	Timestamp int64          `json:"timestamp"` // Unix nano
}

// ── Constructors ──────────────────────────────────────────────────────────────

func newMsg(t MsgType, senderID int, ts []int, payload map[string]any) Message {
	return Message{
		MsgType:   t,
		SenderID:  senderID,
		VectorTS:  ts,
		Payload:   payload,
		MsgID:     genID(),
		Timestamp: time.Now().UnixNano(),
	}
}

func NewHeartbeat(senderID int, ts []int, aliveNodes []int) Message {
	return newMsg(MsgHeartbeat, senderID, ts, map[string]any{"alive_nodes": aliveNodes})
}

func NewSyncRequest(senderID int, ts []int, fromSlot int) Message {
	return newMsg(MsgSyncRequest, senderID, ts, map[string]any{"from_slot": fromSlot})
}

func NewSyncResponse(senderID int, ts []int, entries []any) Message {
	return newMsg(MsgSyncResponse, senderID, ts, map[string]any{"entries": entries})
}

// Ricart-Agrawala
func NewRARequest(senderID int, ts []int, csName string) Message {
	return newMsg(MsgRARequest, senderID, ts, map[string]any{"cs_name": csName})
}

func NewRAReply(senderID int, ts []int, toNode int, csName string) Message {
	return newMsg(MsgRAReply, senderID, ts, map[string]any{"to_node": toNode, "cs_name": csName})
}

// Paxos
func NewPaxosPrepare(senderID int, ts []int, slot, ballot int) Message {
	return newMsg(MsgPaxosPrepare, senderID, ts, map[string]any{"slot": slot, "ballot": ballot})
}

func NewPaxosPromise(senderID int, ts []int, slot, ballot int,
	acceptedBallot *int, acceptedValue map[string]any) Message {
	p := map[string]any{"slot": slot, "ballot": ballot,
		"accepted_ballot": acceptedBallot, "accepted_value": acceptedValue}
	return newMsg(MsgPaxosPromise, senderID, ts, p)
}

func NewPaxosAccept(senderID int, ts []int, slot, ballot int, value map[string]any) Message {
	return newMsg(MsgPaxosAccept, senderID, ts,
		map[string]any{"slot": slot, "ballot": ballot, "value": value})
}

func NewPaxosAccepted(senderID int, ts []int, slot, ballot int) Message {
	return newMsg(MsgPaxosAccepted, senderID, ts, map[string]any{"slot": slot, "ballot": ballot})
}

func NewPaxosCommit(senderID int, ts []int, entry map[string]any) Message {
	return newMsg(MsgPaxosCommit, senderID, ts, map[string]any{"entry": entry})
}

// Game actions
func NewSpeak(senderID int, ts []int, text string) Message {
	return newMsg(MsgSpeak, senderID, ts, map[string]any{"text": text})
}

func NewVote(senderID int, ts []int, targetID int) Message {
	return newMsg(MsgVote, senderID, ts, map[string]any{"target_id": targetID})
}

func NewNightAction(senderID int, ts []int, action string, targetID int) Message {
	return newMsg(MsgNightAction, senderID, ts,
		map[string]any{"action": action, "target_id": targetID})
}

func NewPhaseChange(senderID int, ts []int, newPhase string) Message {
	return newMsg(MsgPhaseChange, senderID, ts, map[string]any{"new_phase": newPhase})
}

// ── Wire encoding: 4-byte length prefix + JSON body ──────────────────────────

// Encode serialises the message to a length-prefixed byte slice.
func (m Message) Encode() ([]byte, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(body)))
	copy(frame[4:], body)
	return frame, nil
}

// ReadMessage reads one length-prefixed message from r.
func ReadMessage(r io.Reader) (Message, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return Message{}, fmt.Errorf("read length: %w", err)
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return Message{}, fmt.Errorf("read body: %w", err)
	}
	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		return Message{}, fmt.Errorf("unmarshal: %w", err)
	}
	return msg, nil
}

// ── Payload helpers (safe type assertions) ───────────────────────────────────

// Int extracts an int-compatible value from a payload field.
func PayloadInt(payload map[string]any, key string) (int, bool) {
	v, ok := payload[key]
	if !ok {
		return 0, false
	}
	switch x := v.(type) {
	case int:
		return x, true
	case float64: // JSON numbers decode as float64
		return int(x), true
	case int64:
		return int(x), true
	}
	return 0, false
}

// String extracts a string from a payload field.
func PayloadStr(payload map[string]any, key string) (string, bool) {
	v, ok := payload[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// ── ID generator ─────────────────────────────────────────────────────────────

var msgCounter atomic.Int64

func genID() string {
	n := msgCounter.Add(1)
	return fmt.Sprintf("msg-%d-%d", n, time.Now().UnixNano())
}
