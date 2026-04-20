// distributed/bully.go — Bully leader election.
package distributed

import (
	"log"
	"mafia-p2p/networking"
	"sync"
	"time"
)

type BullyElection struct {
	nodeID   int
	net      *networking.PeerNetwork
	vc       *VectorClock
	leaderID int
	mu       sync.RWMutex
	stopCh   chan struct{}
	electing bool
	electMu  sync.Mutex
}

func NewBullyElection(nodeID int, net *networking.PeerNetwork, vc *VectorClock) *BullyElection {
	be := &BullyElection{
		nodeID: nodeID,
		net:    net,
		vc:     vc,
		stopCh: make(chan struct{}),
	}
	net.RegisterHandler(networking.MsgElection, be.onElection)
	net.RegisterHandler(networking.MsgAnswer, be.onAnswer)
	net.RegisterHandler(networking.MsgCoordinator, be.onCoordinator)
	return be
}

func (be *BullyElection) Start() {
	go be.monitor()
}

func (be *BullyElection) Stop() {
	close(be.stopCh)
}

func (be *BullyElection) monitor() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-be.stopCh:
			return
		case <-ticker.C:
			be.mu.RLock()
			leader := be.leaderID
			be.mu.RUnlock()
			if leader == -1 || leader == be.nodeID {
				continue
			}
			alive := be.net.AliveNodes()
			if !alive[leader] {
				log.Printf("[Bully] Leader %d appears dead, starting election", leader)
				be.startElection()
			}
		}
	}
}

func (be *BullyElection) startElection() {
	be.electMu.Lock()
	if be.electing {
		be.electMu.Unlock()
		return
	}
	be.electing = true
	be.electMu.Unlock()
	defer func() {
		be.electMu.Lock()
		be.electing = false
		be.electMu.Unlock()
	}()

	higherNodes := be.getHigherIDs()
	if len(higherNodes) == 0 {
		be.declareVictory()
		return
	}

	ts := be.vc.Tick()
	msg := networking.NewElection(be.nodeID, ts)
	for _, id := range higherNodes {
		be.net.Send(id, msg)
	}

	time.Sleep(2 * time.Second)
	be.mu.Lock()
	if be.leaderID == -1 || be.leaderID != be.nodeID {
		be.declareVictory()
	}
	be.mu.Unlock()
}

func (be *BullyElection) onElection(msg networking.Message) {
	be.vc.Update(msg.VectorTS)
	ts := be.vc.Tick()
	answer := networking.NewAnswer(be.nodeID, ts)
	be.net.Send(msg.SenderID, answer)
	be.startElection()
}

func (be *BullyElection) onAnswer(msg networking.Message) {
	be.vc.Update(msg.VectorTS)
	// Wait for coordinator message
}

func (be *BullyElection) onCoordinator(msg networking.Message) {
	be.vc.Update(msg.VectorTS)
	leaderID, _ := networking.PayloadInt(msg.Payload, "leader_id")
	be.mu.Lock()
	be.leaderID = leaderID
	be.mu.Unlock()
	log.Printf("[Bully] New leader elected: %d", leaderID)
}

func (be *BullyElection) declareVictory() {
	be.mu.Lock()
	be.leaderID = be.nodeID
	be.mu.Unlock()
	ts := be.vc.Tick()
	msg := networking.NewCoordinator(be.nodeID, ts, be.nodeID)
	be.net.Broadcast(msg)
	log.Printf("[Bully] I am the new leader (node %d)", be.nodeID)
}

func (be *BullyElection) getHigherIDs() []int {
	alive := be.net.AliveNodes()
	var higher []int
	for id, isAlive := range alive {
		if id > be.nodeID && isAlive {
			higher = append(higher, id)
		}
	}
	return higher
}

func (be *BullyElection) GetLeader() int {
	be.mu.RLock()
	defer be.mu.RUnlock()
	return be.leaderID
}
