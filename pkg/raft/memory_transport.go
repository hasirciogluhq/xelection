package raft

import (
	"context"
	"fmt"
	"sync"
)

// InMemoryTransport is a simple in-memory transport for testing
type InMemoryTransport struct {
	nodeID         NodeID
	messageHandler MessageHandler
	peers          map[NodeID]*InMemoryTransport
	mu             sync.RWMutex
}

// NewInMemoryTransport creates a new in-memory transport
func NewInMemoryTransport(nodeID NodeID) *InMemoryTransport {
	return &InMemoryTransport{
		nodeID: nodeID,
		peers:  make(map[NodeID]*InMemoryTransport),
	}
}

// Start starts the in-memory transport
func (t *InMemoryTransport) Start(ctx context.Context) error {
	return nil
}

// Stop stops the in-memory transport
func (t *InMemoryTransport) Stop() error {
	return nil
}

// Send sends a message to a specific node
func (t *InMemoryTransport) Send(ctx context.Context, to NodeID, msg Message) error {
	t.mu.RLock()
	peer, exists := t.peers[to]
	handler := peer.messageHandler
	t.mu.RUnlock()

	if !exists {
		return ErrPeerNotFound
	}

	// Call handler in a separate goroutine to avoid deadlock
	if handler != nil {
		fmt.Printf("📨 Delivering message from %s to %s (type: %d)\n", msg.From, to, msg.Type)
		go handler(msg.From, msg)
	}

	return nil
}

// Broadcast sends a message to all nodes in the cluster
func (t *InMemoryTransport) Broadcast(ctx context.Context, msg Message) error {
	t.mu.RLock()
	handlers := make([]func(NodeID, Message), 0)
	for peerID, peer := range t.peers {
		if peerID != t.nodeID && peer.messageHandler != nil {
			handlers = append(handlers, peer.messageHandler)
		}
	}
	t.mu.RUnlock()

	// Call handlers in separate goroutines to avoid deadlock
	for _, handler := range handlers {
		go handler(msg.From, msg)
	}

	return nil
}

// SetMessageHandler sets the callback for incoming messages
func (t *InMemoryTransport) SetMessageHandler(handler MessageHandler) {
	t.messageHandler = handler
}

// AddPeer adds a peer to the transport
func (t *InMemoryTransport) AddPeer(peer *InMemoryTransport) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[peer.nodeID] = peer
}

// ConnectPeers connects multiple transports together
func ConnectPeers(transports ...*InMemoryTransport) {
	for _, t1 := range transports {
		for _, t2 := range transports {
			if t1.nodeID != t2.nodeID {
				t1.AddPeer(t2)
			}
		}
	}
}

// Errors
var (
	ErrPeerNotFound = &raftError{"peer not found"}
)
