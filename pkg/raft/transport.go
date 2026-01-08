package raft

import "context"

// Transport defines the interface that developers must implement
// to handle network communication between Raft nodes
type Transport interface {
	// Send sends a message to a specific node
	Send(ctx context.Context, to NodeID, msg Message) error

	// Broadcast sends a message to all nodes in the cluster
	Broadcast(ctx context.Context, msg Message) error

	// Start starts the transport layer
	Start(ctx context.Context) error

	// Stop stops the transport layer
	Stop() error

	// SetMessageHandler sets the callback for incoming messages
	SetMessageHandler(handler MessageHandler)
}

// MessageHandler handles incoming messages from other nodes
type MessageHandler func(from NodeID, msg Message)

// NodeID uniquely identifies a node in the cluster
type NodeID string
