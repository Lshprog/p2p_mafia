// networking/peer_network.go — TCP-based P2P network layer.
//
// Each node:
//   - Listens on its own port (goroutine server loop).
//   - Maintains outgoing connections to all other nodes (goroutine connector).
//   - Dispatches received messages to registered handler callbacks.
//   - Runs a heartbeat goroutine and a failure-detector goroutine.
//
// Wire protocol: 4-byte big-endian length prefix + UTF-8 JSON body.
package networking

import (
	"fmt"
	"log"
	"mafia-p2p/config"
	"net"
	"sync"
	"time"
)

// Handler is a function that processes an incoming message.
type Handler func(Message)

// PeerNetwork manages all TCP connections for one node.
type PeerNetwork struct {
	NodeID int

	// Handler registry: MsgType → []Handler
	mu       sync.RWMutex
	handlers map[MsgType][]Handler

	// Outgoing connections keyed by peer node ID
	peersMu sync.Mutex
	peers   map[int]net.Conn

	// Liveness tracking
	aliveMu  sync.RWMutex
	alive    map[int]bool
	lastSeen map[int]time.Time

	// Callbacks
	vcSnapshotFn    func() []int // injected by PlayerNode
	nodeDeadFn      func(int)    // injected by PlayerNode
	committedSlotFn func() int

	listener net.Listener
	stopCh   chan struct{}
	once     sync.Once
}

// NewPeerNetwork creates (but does not start) a PeerNetwork for nodeID.
func NewPeerNetwork(nodeID int) *PeerNetwork {
	alive := make(map[int]bool, config.NumNodes)
	last := make(map[int]time.Time, config.NumNodes)
	now := time.Now()
	for id := 0; id < config.NumNodes; id++ {
		if id != nodeID {
			alive[id] = true
			last[id] = now
		}
	}
	return &PeerNetwork{
		NodeID:   nodeID,
		handlers: make(map[MsgType][]Handler),
		peers:    make(map[int]net.Conn),
		alive:    alive,
		lastSeen: last,
		stopCh:   make(chan struct{}),
	}
}

// ── Lifecycle ─────────────────────────────────────────────────────────────────

// Start launches the server, connector, heartbeat, and failure-detector goroutines.
func (n *PeerNetwork) Start() error {
	addr := n.NodeRegistry()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	n.listener = ln
	log.Printf("[Node %d] Listening on %s", n.NodeID, addr)

	go n.serve()
	go n.connectLoop()
	go n.heartbeatLoop()
	go n.failureDetector()
	return nil
}

// Stop shuts down all connections and goroutines.
func (n *PeerNetwork) Stop() {
	n.once.Do(func() {
		close(n.stopCh)
		if n.listener != nil {
			n.listener.Close()
		}
		n.peersMu.Lock()
		for _, c := range n.peers {
			c.Close()
		}
		n.peersMu.Unlock()
	})
}

// NodeRegistry returns the "host:port" string for this node.
func (n *PeerNetwork) NodeRegistry() string {
	a := config.NodeRegistry[n.NodeID]
	return fmt.Sprintf("%s:%d", a.Host, a.Port)
}

// ── Handler registration ──────────────────────────────────────────────────────

// RegisterHandler adds a handler for the given message type.
func (n *PeerNetwork) RegisterHandler(t MsgType, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[t] = append(n.handlers[t], h)
}

// SetVCCallback injects the Vector Clock snapshot function used by heartbeats.
func (n *PeerNetwork) SetVCCallback(fn func() []int) { n.vcSnapshotFn = fn }

// SetNodeDeadCallback is called when a peer is declared dead.
func (n *PeerNetwork) SetNodeDeadCallback(fn func(int)) { n.nodeDeadFn = fn }

func (n *PeerNetwork) SetCommittedSlotCallback(fn func() int) {
	n.committedSlotFn = fn
}

// ── Send / Broadcast ──────────────────────────────────────────────────────────

// Send unicasts msg to peerID. Returns false if the peer is unreachable.
func (n *PeerNetwork) Send(peerID int, msg Message) bool {
	n.peersMu.Lock()
	conn, ok := n.peers[peerID]
	n.peersMu.Unlock()
	if !ok {
		return false
	}
	data, err := msg.Encode()
	if err != nil {
		log.Printf("[Node %d] encode error: %v", n.NodeID, err)
		return false
	}
	if _, err := conn.Write(data); err != nil {
		log.Printf("[Node %d] send to %d failed: %v", n.NodeID, peerID, err)
		n.markDead(peerID)
		return false
	}
	return true
}

// Broadcast sends msg to all currently alive peers.
func (n *PeerNetwork) Broadcast(msg Message) {
	n.aliveMu.RLock()
	targets := make([]int, 0, len(n.alive))
	for id, isAlive := range n.alive {
		if isAlive {
			targets = append(targets, id)
		}
	}
	n.aliveMu.RUnlock()
	for _, id := range targets {
		n.Send(id, msg)
	}
}

// AliveNodes returns the set of currently alive peer IDs (excluding self).
func (n *PeerNetwork) AliveNodes() map[int]bool {
	n.aliveMu.RLock()
	defer n.aliveMu.RUnlock()
	result := make(map[int]bool, len(n.alive))
	for id, alive := range n.alive {
		result[id] = alive
	}
	return result
}

// ConnectedPeerCount returns the number of peers with active TCP connections.
func (n *PeerNetwork) ConnectedPeerCount() int {
	n.peersMu.Lock()
	defer n.peersMu.Unlock()
	return len(n.peers)
}

// ── Server (inbound) ──────────────────────────────────────────────────────────

func (n *PeerNetwork) serve() {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			select {
			case <-n.stopCh:
				return
			default:
				log.Printf("[Node %d] accept error: %v", n.NodeID, err)
				continue
			}
		}
		go n.handleConn(conn)
	}
}

func (n *PeerNetwork) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		msg, err := ReadMessage(conn)
		if err != nil {
			return
		}
		n.dispatch(msg)
	}
}

func (n *PeerNetwork) dispatch(msg Message) {
	// Update liveness
	n.aliveMu.Lock()
	n.lastSeen[msg.SenderID] = time.Now()
	n.alive[msg.SenderID] = true
	n.aliveMu.Unlock()

	n.mu.RLock()
	hs := n.handlers[msg.MsgType]
	n.mu.RUnlock()
	for _, h := range hs {
		go h(msg)
	}
}

// ── Connector (outbound) ──────────────────────────────────────────────────────

func (n *PeerNetwork) connectLoop() {
	for {
		select {
		case <-n.stopCh:
			return
		case <-time.After(2 * time.Second):
		}
		for id, addr := range config.NodeRegistry {
			if id == n.NodeID {
				continue
			}
			n.peersMu.Lock()
			_, connected := n.peers[id]
			n.peersMu.Unlock()
			if connected {
				continue
			}
			target := fmt.Sprintf("%s:%d", addr.Host, addr.Port)
			conn, err := net.DialTimeout("tcp", target, 2*time.Second)
			if err != nil {
				continue
			}
			n.peersMu.Lock()
			n.peers[id] = conn
			n.peersMu.Unlock()
			log.Printf("[Node %d] Connected to peer %d", n.NodeID, id)
		}
	}
}

// ── Heartbeat & failure detection ─────────────────────────────────────────────

func (n *PeerNetwork) heartbeatLoop() {
	ticker := time.NewTicker(config.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			var ts []int
			if n.vcSnapshotFn != nil {
				ts = n.vcSnapshotFn()
			} else {
				ts = make([]int, config.NumNodes)
			}

			committed := 0
			if n.committedSlotFn != nil {
				committed = n.committedSlotFn()
			}

			n.aliveMu.RLock()
			alive := make([]int, 0, len(n.alive))
			for id, a := range n.alive {
				if a {
					alive = append(alive, id)
				}
			}
			n.aliveMu.RUnlock()

			hb := NewHeartbeat(n.NodeID, ts, alive, committed)
			n.Broadcast(hb)
		}
	}
}

func (n *PeerNetwork) failureDetector() {
	ticker := time.NewTicker(config.NodeTimeout / 2)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.aliveMu.Lock()
			now := time.Now()
			for id, last := range n.lastSeen {
				if n.alive[id] && now.Sub(last) > config.NodeTimeout {
					n.alive[id] = false
					go n.markDead(id)
				}
			}
			n.aliveMu.Unlock()
		}
	}
}

func (n *PeerNetwork) markDead(peerID int) {
	n.aliveMu.Lock()
	n.alive[peerID] = false
	n.aliveMu.Unlock()

	n.peersMu.Lock()
	if conn, ok := n.peers[peerID]; ok {
		conn.Close()
		delete(n.peers, peerID)
	}
	n.peersMu.Unlock()

	log.Printf("[Node %d] Peer %d marked DEAD", n.NodeID, peerID)
	if n.nodeDeadFn != nil {
		n.nodeDeadFn(peerID)
	}
}
