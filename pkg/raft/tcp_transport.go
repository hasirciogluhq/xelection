package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

// TCPTransport is an example TCP implementation of the Transport interface
type TCPTransport struct {
	nodeID         NodeID
	listener       net.Listener
	connections    map[NodeID]net.Conn
	mu             sync.RWMutex
	messageHandler MessageHandler
	ctx            context.Context
	cancel         context.CancelFunc
}

// TCPTransportConfig holds configuration for TCP transport
type TCPTransportConfig struct {
	NodeID     NodeID
	ListenAddr string
	Peers      map[NodeID]string // NodeID -> address
}

// NewTCPTransport creates a new TCP transport
func NewTCPTransport(config TCPTransportConfig) *TCPTransport {
	ctx, cancel := context.WithCancel(context.Background())

	return &TCPTransport{
		nodeID:      config.NodeID,
		connections: make(map[NodeID]net.Conn),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Start starts the TCP transport
func (t *TCPTransport) Start(ctx context.Context) error {
	listener, err := net.Listen("tcp", ":4040")
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	t.listener = listener

	// Start accepting connections
	go t.acceptConnections()

	return nil
}

// Stop stops the TCP transport
func (t *TCPTransport) Stop() error {
	t.cancel()

	if t.listener != nil {
		t.listener.Close()
	}

	t.mu.Lock()
	for _, conn := range t.connections {
		conn.Close()
	}
	t.mu.Unlock()

	return nil
}

// Send sends a message to a specific node
func (t *TCPTransport) Send(ctx context.Context, to NodeID, msg Message) error {
	t.mu.RLock()
	conn, exists := t.connections[to]
	t.mu.RUnlock()

	if !exists {
		// Try to establish connection
		addr := t.getPeerAddress(to)
		if addr == "" {
			return fmt.Errorf("unknown peer address for %s", to)
		}

		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			return fmt.Errorf("failed to connect to %s: %w", to, err)
		}

		t.mu.Lock()
		t.connections[to] = conn
		t.mu.Unlock()
	}

	// Serialize message
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Send message
	_, err = conn.Write(append(data, '\n'))
	if err != nil {
		t.mu.Lock()
		delete(t.connections, to)
		t.mu.Unlock()
		return fmt.Errorf("failed to send message: %w", err)
	}

	return nil
}

// Broadcast sends a message to all nodes in the cluster
func (t *TCPTransport) Broadcast(ctx context.Context, msg Message) error {
	// TODO: Implement broadcast to all known peers
	return nil
}

// SetMessageHandler sets the callback for incoming messages
func (t *TCPTransport) SetMessageHandler(handler MessageHandler) {
	t.messageHandler = handler
}

// acceptConnections accepts incoming connections
func (t *TCPTransport) acceptConnections() {
	for {
		select {
		case <-t.ctx.Done():
			return
		default:
			conn, err := t.listener.Accept()
			if err != nil {
				continue
			}

			go t.handleConnection(conn)
		}
	}
}

// handleConnection handles an incoming connection
func (t *TCPTransport) handleConnection(conn net.Conn) {
	defer conn.Close()

	decoder := json.NewDecoder(conn)

	for {
		select {
		case <-t.ctx.Done():
			return
		default:
			var msg Message
			err := decoder.Decode(&msg)
			if err != nil {
				return
			}

			if t.messageHandler != nil {
				t.messageHandler(msg.From, msg)
			}
		}
	}
}

// getPeerAddress returns the address for a peer
func (t *TCPTransport) getPeerAddress(peer NodeID) string {
	// TODO: Implement peer address lookup
	return ""
}
