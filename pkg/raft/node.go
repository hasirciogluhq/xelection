package raft

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// NodeState represents the state of a Raft node
type NodeState int

const (
	NodeStateFollower NodeState = iota
	NodeStateCandidate
	NodeStateLeader
)

// Node represents a Raft node in the cluster
type Node struct {
	id        NodeID
	state     NodeState
	term      uint64
	votedFor  NodeID
	transport Transport

	// Election timeout
	electionTimeout time.Duration
	lastHeartbeat   time.Time

	// Leadership
	leaderID NodeID

	// Cluster members
	clusterMembers []NodeID

	// Voting
	votesReceived map[NodeID]bool

	// Log
	log         []LogEntry
	commitIndex uint64
	lastApplied uint64

	// Synchronization
	mu sync.RWMutex

	// Context
	ctx    context.Context
	cancel context.CancelFunc
}

// Config holds configuration for a Raft node
type Config struct {
	ID              NodeID
	Transport       Transport
	ElectionTimeout time.Duration
	ClusterMembers  []NodeID
}

// NewNode creates a new Raft node
func NewNode(config Config) *Node {
	ctx, cancel := context.WithCancel(context.Background())

	node := &Node{
		id:              config.ID,
		state:           NodeStateFollower,
		term:            0,
		votedFor:        "",
		transport:       config.Transport,
		electionTimeout: config.ElectionTimeout,
		lastHeartbeat:   time.Now(),
		clusterMembers:  config.ClusterMembers,
		votesReceived:   make(map[NodeID]bool),
		log:             make([]LogEntry, 0),
		commitIndex:     0,
		lastApplied:     0,
		ctx:             ctx,
		cancel:          cancel,
	}

	// Set message handler
	config.Transport.SetMessageHandler(node.handleMessage)

	return node
}

// Start starts the Raft node
func (n *Node) Start() error {
	go n.electionLoop()
	return n.transport.Start(n.ctx)
}

// Stop stops the Raft node
func (n *Node) Stop() error {
	n.cancel()
	return n.transport.Stop()
}

// GetState returns the current state of the node
func (n *Node) GetState() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// GetTerm returns the current term of the node
func (n *Node) GetTerm() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.term
}

// GetLeader returns the current leader ID
func (n *Node) GetLeader() NodeID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.leaderID
}

// GetID returns the node ID
func (n *Node) GetID() NodeID {
	return n.id
}

// handleMessage handles incoming messages from other nodes
func (n *Node) handleMessage(from NodeID, msg Message) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Ignore messages from older terms
	if msg.Term < n.term {
		return
	}

	// Update term if we received a message from a higher term
	if msg.Term > n.term {
		n.term = msg.Term
		n.votedFor = ""
		n.state = NodeStateFollower
		n.leaderID = ""
	}

	switch msg.Type {
	case MessageTypeVoteRequest:
		n.handleVoteRequest(from, msg)
	case MessageTypeVoteResponse:
		n.handleVoteResponse(from, msg)
	case MessageTypeAppendEntriesRequest:
		n.handleAppendEntriesRequest(from, msg)
	case MessageTypeAppendEntriesResponse:
		n.handleAppendEntriesResponse(from, msg)
	}
}

// handleVoteRequest handles vote request messages
func (n *Node) handleVoteRequest(from NodeID, msg Message) {
	request, ok := msg.Payload.(VoteRequest)
	if !ok {
		return
	}

	response := VoteResponse{
		Term: n.term,
	}

	// If request term is higher, update our term and become follower
	if request.Term > n.term {
		n.term = request.Term
		n.state = NodeStateFollower
		n.votedFor = ""
		n.lastHeartbeat = time.Now()
	}

	// Grant vote if conditions are met
	if request.Term == n.term &&
		(n.votedFor == "" || n.votedFor == request.CandidateID) &&
		n.isLogUpToDate(request.LastLogIndex, request.LastLogTerm) {
		response.VoteGranted = true
		n.votedFor = request.CandidateID
		n.lastHeartbeat = time.Now()
		fmt.Printf("✅ Node %s voting for %s in term %d\n", n.id, request.CandidateID, n.term)
	} else {
		fmt.Printf("❌ Node %s rejecting vote for %s in term %d (votedFor: %s)\n", n.id, request.CandidateID, n.term, n.votedFor)
	}

	// Send response
	n.transport.Send(n.ctx, from, Message{
		Type:    MessageTypeVoteResponse,
		Term:    n.term,
		From:    n.id,
		To:      from,
		Payload: response,
	})
}

// handleVoteResponse handles vote response messages
func (n *Node) handleVoteResponse(from NodeID, msg Message) {
	if n.state != NodeStateCandidate {
		return
	}

	response, ok := msg.Payload.(VoteResponse)
	if !ok {
		return
	}

	if response.Term > n.term {
		n.term = response.Term
		n.state = NodeStateFollower
		n.votedFor = ""
		return
	}

	if response.VoteGranted {
		n.votesReceived[from] = true

		fmt.Printf("🗳️ Node %s received vote from %s (total: %d/%d)\n",
			n.id, from, len(n.votesReceived), len(n.clusterMembers)/2+1)

		// Check if we have majority (need > half)
		if len(n.votesReceived) > len(n.clusterMembers)/2 {
			n.becomeLeader()
		}
	}
}

// handleAppendEntriesRequest handles append entries request messages
func (n *Node) handleAppendEntriesRequest(from NodeID, msg Message) {
	request, ok := msg.Payload.(AppendEntriesRequest)
	if !ok {
		return
	}

	// Reset election timeout on receiving append entries from leader
	n.lastHeartbeat = time.Now()

	response := AppendEntriesResponse{
		Term: n.term,
	}

	// If term is higher, update our term and become follower
	if request.Term > n.term {
		n.term = request.Term
		n.state = NodeStateFollower
		n.votedFor = ""
	}

	// Accept leadership if we're not leader
	if n.state != NodeStateLeader {
		if n.leaderID != request.LeaderID {
			fmt.Printf("👑 Node %s recognizing %s as leader in term %d\n", n.id, request.LeaderID, request.Term)
		}
		n.leaderID = request.LeaderID
	}

	// TODO: Implement full append entries logic with log consistency check
	response.Success = true

	// Send response
	n.transport.Send(n.ctx, from, Message{
		Type:    MessageTypeAppendEntriesResponse,
		Term:    n.term,
		From:    n.id,
		To:      from,
		Payload: response,
	})
}

// handleAppendEntriesResponse handles append entries response messages
func (n *Node) handleAppendEntriesResponse(from NodeID, msg Message) {
	// TODO: Implement append entries response logic
}

// startElection starts a new election
func (n *Node) startElection() {
	n.state = NodeStateCandidate
	n.term++
	n.votedFor = n.id
	n.lastHeartbeat = time.Now()
	n.votesReceived = make(map[NodeID]bool)
	n.votesReceived[n.id] = true

	fmt.Printf("🗳️ Node %s starting election for term %d\n", n.id, n.term)

	// Get last log index and term
	lastLogIndex := uint64(0)
	lastLogTerm := uint64(0)
	if len(n.log) > 0 {
		lastLogIndex = n.log[len(n.log)-1].Index
		lastLogTerm = n.log[len(n.log)-1].Term
	}

	// Send vote requests to all other nodes
	request := VoteRequest{
		Term:         n.term,
		CandidateID:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, member := range n.clusterMembers {
		if member != n.id {
			fmt.Printf("📤 Node %s requesting vote from %s\n", n.id, member)
			n.transport.Send(n.ctx, member, Message{
				Type:    MessageTypeVoteRequest,
				Term:    n.term,
				From:    n.id,
				To:      member,
				Payload: request,
			})
		}
	}

	// Check if we already have majority (single node cluster)
	if len(n.clusterMembers) == 1 {
		n.becomeLeader()
	}
}

// becomeLeader transitions the node to leader state
func (n *Node) becomeLeader() {
	n.state = NodeStateLeader
	n.leaderID = n.id

	fmt.Printf("👑 Node %s became leader in term %d\n", n.id, n.term)

	// Start heartbeat loop
	go n.heartbeatLoop()

	// Send initial empty append entries to establish leadership
	for _, member := range n.clusterMembers {
		if member != n.id {
			go n.sendAppendEntries(member)
		}
	}
}

// isLogUpToDate checks if our log is up-to-date with the candidate
func (n *Node) isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm uint64) bool {
	ourLastLogTerm := uint64(0)
	ourLastLogIndex := uint64(0)

	if len(n.log) > 0 {
		ourLastLogTerm = n.log[len(n.log)-1].Term
		ourLastLogIndex = n.log[len(n.log)-1].Index
	}

	if candidateLastLogTerm > ourLastLogTerm {
		return true
	}

	if candidateLastLogTerm == ourLastLogTerm && candidateLastLogIndex >= ourLastLogIndex {
		return true
	}

	return false
}

// heartbeatLoop sends regular heartbeats as leader
func (n *Node) heartbeatLoop() {
	// Send heartbeats at half the election timeout
	heartbeatInterval := n.electionTimeout / 6
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()

			// Only send heartbeats if we're still the leader
			if n.state == NodeStateLeader {
				for _, member := range n.clusterMembers {
					if member != n.id {
						go n.sendAppendEntries(member)
					}
				}
			} else {
				// We're no longer leader, stop the heartbeat loop
				n.mu.Unlock()
				return
			}

			n.mu.Unlock()
		}
	}
}

// electionLoop runs the election timeout logic
func (n *Node) electionLoop() {
	ticker := time.NewTicker(n.electionTimeout / 3)
	defer ticker.Stop()

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()

			// Check if election timeout has expired
			if time.Since(n.lastHeartbeat) > n.electionTimeout {
				if n.state != NodeStateLeader {
					n.startElection()
				}
			}

			n.mu.Unlock()
		}
	}
}
