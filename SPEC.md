# Xelection - Raft Consensus Library

Xelection is a Go library that implements the Raft consensus algorithm with transport layer abstraction. The library provides both node-level and partition-level leader election capabilities.

## Features

- **Transport Layer Abstraction**: Developers implement their own transport layer
- **Node-level Elections**: Standard Raft leader election across cluster nodes
- **Partition-level Elections**: CockroachDB-style partition-based elections
- **Log Replication**: Distributed log replication with consistency guarantees
- **Simple API**: Easy-to-use interface for integrating Raft into applications

## Architecture

### Core Components

1. **Transport Interface**: Abstract interface for network communication
2. **Node**: Raft node implementation with state machine
3. **PartitionNode**: Extended node supporting partition-level elections
4. **Message Types**: Raft protocol messages (vote requests, append entries, etc.)

### Transport Interface

Developers must implement the `Transport` interface:

```go
type Transport interface {
    Send(ctx context.Context, to NodeID, msg Message) error
    Broadcast(ctx context.Context, msg Message) error
    Start(ctx context.Context) error
    Stop() error
    SetMessageHandler(handler MessageHandler)
}
```

### Usage Example

```go
// Create transport
transport := NewTCPTransport(config)

// Create Raft node
config := raft.Config{
    ID:              "node1",
    Transport:       transport,
    ElectionTimeout: 5 * time.Second,
    ClusterMembers:  []NodeID{"node1", "node2", "node3"},
}

node := raft.NewNode(config)
node.Start()

// Propose data if leader
if node.GetState() == raft.NodeStateLeader {
    err := node.Propose("test data")
}
```

### Partition-level Elections

```go
partitionConfigs := []raft.PartitionConfig{
    {ID: "partition1", Members: []NodeID{"node1", "node2"}},
    {ID: "partition2", Members: []NodeID{"node2", "node3"}},
}

partitionNode := raft.NewPartitionNode(config, partitionConfigs)

// Check partition leadership
leader := partitionNode.GetPartitionLeader("partition1")
```

## Implementation Status

- [x] Transport interface abstraction
- [x] Core Raft node implementation
- [x] Leader election logic
- [x] Log replication mechanism
- [x] Partition-level election support
- [x] Example TCP transport implementation
- [ ] Comprehensive tests
- [ ] Performance benchmarks
- [ ] Full documentation

## Next Steps

1. Add comprehensive unit and integration tests
2. Implement log persistence
3. Add snapshot support
4. Performance optimization
5. Add monitoring and metrics