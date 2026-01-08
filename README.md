# Xelection - Go Raft Consensus Library

Xelection is a comprehensive Go library that provides a robust implementation of the Raft consensus algorithm. It is designed with flexibility in mind, offering a pluggable transport layer and supporting both standard node-level leader elections and advanced CockroachDB-style partition-level elections.

## Features

-   **Raft Consensus**: Full implementation of the Raft consensus algorithm, including leader election, log replication, and commit mechanisms.
-   **Pluggable Transport Layer**: Abstract `Transport` interface allows developers to integrate various network communication protocols (TCP, in-memory, custom).
-   **Node-level Elections**: Standard Raft leader election across the entire cluster of nodes.
-   **Partition-level Elections**: Support for CockroachDB-inspired partition-based elections, enabling more granular control and fault tolerance in distributed systems.
-   **Log Replication**: Ensures strong consistency across the cluster by replicating log entries to all followers.
-   **RPC Framework**: Integrated RPC handling for inter-node communication and request/response mechanisms.
-   **Unified Packet Structure**: A generic `Packet` type for consistent data exchange across the transport layer.
-   **Snowflake ID Generation**: Utilizes a Snowflake-like algorithm for generating unique, distributed IDs.
-   **Structured Logging**: Colored and categorized logging for improved debuggability and operational insights.

## Architecture

The library is structured into several key packages, each responsible for a distinct aspect of the consensus and communication system:

### `pkg/raft`

This package contains the core implementation of the Raft consensus algorithm.

-   **`node.go`**: Defines the `Node` structure, representing a Raft node. It manages the node's state (Follower, Candidate, Leader), term, voted-for candidate, election timeouts, and the Raft log. It orchestrates leader election, heartbeat mechanisms, and log replication.
-   **`transport.go`**: Declares the `Transport` interface, which is a crucial abstraction for network communication. Any custom transport implementation must adhere to this interface.
-   **`partition.go`**: Introduces the `PartitionNode`, extending the base Raft node to handle partition-level elections. This allows a single physical node to participate in multiple independent Raft groups, each managing a subset of data or services.
-   **`types.go`**: Houses fundamental data types such as `NodeID`, `NodeState`, `Config` (for node configuration), and `LogEntry` (for the Raft log).
-   **`message.go`**: Defines the various message types exchanged between Raft nodes, including `VoteRequest`, `VoteResponse`, `AppendEntriesRequest`, and `AppendEntriesResponse`.
-   **`log.go`**: Provides functions for proposing new commands to the Raft log (`Propose`) and replicating log entries to follower nodes.
-   **`tcp_transport.go` (in `pkg/raft`)**: A concrete implementation of the `Transport` interface using TCP sockets, demonstrating how to integrate network communication with the Raft core.
-   **`memory_transport.go`**: An in-memory implementation of the `Transport` interface, primarily used for testing and simulating Raft clusters within a single process.

### `pkg/transport`

This package handles the generic network communication infrastructure, separate from the Raft-specific transport.

-   **`manager.go`**: The `TransportManager` is responsible for managing multiple transport clients, registering packet handlers, and dispatching incoming data to the appropriate services.
-   **`tcp_transport.go` (in `pkg/transport`)**: A more general-purpose TCP transport implementation for establishing and managing client connections. It uses a length-prefixed framing protocol for reliable data transmission and incorporates TCP keep-alive mechanisms.
-   **`client.go`**: Represents a `Client` connection, encapsulating its unique ID, connection direction (incoming/outgoing), and a flexible context map for storing client-specific data. It also includes methods for type-safe context retrieval and disconnection.
-   **`transport.go`**: Defines interfaces for `TransportClientDispatcher` (for handling connection events and data), `PacketHandler` (for processing incoming packets), `TransportDialer` (for initiating outgoing connections), and the generic `Transport` interface.

### `pkg/packet`

This package defines the universal data structure for inter-component communication.

-   **`packet.go`**: The `Packet` struct provides a standardized format for messages, including an ID, `PacketType` (e.g., `EVENT`, `RPC`), metadata, payload, and a timestamp. It includes utility functions for marshaling and unmarshaling packets to and from JSON.

### `pkg/rpc`

This package provides a basic RPC framework for handling remote procedure calls.

-   **`store.go`**: The `RpcHandler` and `Store` manage registered RPC handlers and their timeouts. It facilitates the dispatching of RPC requests and ensures timely responses.

### `pkg/logger`

A simple yet effective logging utility.

-   **`logger.go`**: Offers colored console output for different log levels (Debug, Info, Warn, Error), enhancing readability during development and operation. Debug logs are conditionally enabled via an environment variable.

### `pkg/utils`

Utility functions for common tasks.

-   **`snowflake.go`**: Implements a Snowflake ID generator, providing globally unique, time-sortable 64-bit integers.

## Getting Started

### Prerequisites

-   Go 1.25.1 or newer

### Installation

To use Xelection in your Go project, simply run:

```bash
go get github.com/hasirciogluhq/xelection
```

### Examples

The `examples/` directory contains demonstration applications to help you understand how to use the Xelection library.

#### Cluster Example (`examples/cluster/main.go`)

This example showcases how to set up a Raft cluster with TCP transport and demonstrates both node-level and partition-level elections.

```go
package main

import (
	"fmt"
	"time"

	"github.com/hasirciogluhq/xelection/pkg/raft"
)

func main() {
	// Create TCP transport for node 1
	tcpConfig1 := raft.TCPTransportConfig{
		NodeID:     "node1",
		ListenAddr: ":4041",
		Peers: map[raft.NodeID]string{
			"node2": "localhost:4042",
			"node3": "localhost:4043",
		},
	}

	transport1 := raft.NewTCPTransport(tcpConfig1)

	// Create Raft node 1
	config1 := raft.Config{
		ID:              "node1",
		Transport:       transport1,
		ElectionTimeout: 300 * time.Millisecond,
		ClusterMembers:  []raft.NodeID{"node1", "node2", "node3"},
	}

	node1 := raft.NewNode(config1)

	// Start the node
	err := node1.Start()
	if err != nil {
		panic(err)
	}
	defer node1.Stop()

	// Create partition-aware node with partitions
	partitionConfigs := []raft.PartitionConfig{
		{
			ID:              "partition1",
			Members:         []raft.NodeID{"node1", "node2"},
			ElectionTimeout: 450 * time.Millisecond,
		},
		{
			ID:              "partition2",
			Members:         []raft.NodeID{"node2", "node3"},
			ElectionTimeout: 280 * time.Millisecond,
		},
	}

	partitionNode := raft.NewPartitionNode(config1, partitionConfigs)

	// Start the partition node
	err = partitionNode.Start()
	if err != nil {
		panic(err)
	}
	defer partitionNode.Stop()

	// Demonstrate usage
	fmt.Printf("Node %s started\n", node1.GetID())
	fmt.Printf("Current state: %v\n", node1.GetState())
	fmt.Printf("Current term: %d\n", node1.GetTerm())

	// Wait for leader election
	time.Sleep(3 * time.Second)

	fmt.Printf("Leader: %s\n", node1.GetLeader())
	fmt.Printf("Partition1 leader: %s\n", partitionNode.GetPartitionLeader("partition1"))
	fmt.Printf("Partition2 leader: %s\n", partitionNode.GetPartitionLeader("partition2"))

	// If this node is leader, propose some data
	if node1.GetState() == raft.NodeStateLeader {
		err = node1.Propose("test data")
		if err != nil {
			fmt.Printf("Failed to propose data: %v\n", err)
		} else {
			fmt.Println("Successfully proposed data")
		}
	}

	// Keep running for demonstration
	select {}
}
```

#### In-Memory Test Example (`examples/memory_test/main.go`)

This example demonstrates how to set up a Raft cluster using the in-memory transport for quick testing and simulation.

```go
package main

import (
	"fmt"
	"time"

	"github.com/hasirciogluhq/xelection/pkg/raft"
)

func main() {
	fmt.Println("🚀 Starting Raft cluster test with in-memory transport...")

	// Create 3 in-memory transports
	transport1 := raft.NewInMemoryTransport("node1")
	transport2 := raft.NewInMemoryTransport("node2")
	transport3 := raft.NewInMemoryTransport("node3")

	// Connect all transports together
	raft.ConnectPeers(transport1, transport2, transport3)

	// Create 3 Raft nodes with different election timeouts to avoid tie
	config1 := raft.Config{
		ID:              "node1",
		Transport:       transport1,
		ElectionTimeout: 100 * time.Millisecond,
		ClusterMembers:  []raft.NodeID{"node1", "node2", "node3"},
	}

	config2 := raft.Config{
		ID:              "node2",
		Transport:       transport2,
		ElectionTimeout: 280 * time.Millisecond,
		ClusterMembers:  []raft.NodeID{"node1", "node2", "node3"},
	}

	config3 := raft.Config{
		ID:              "node3",
		Transport:       transport3,
		ElectionTimeout: 145 * time.Millisecond,
		ClusterMembers:  []raft.NodeID{"node1", "node2", "node3"},
	}

	// Create nodes
	node1 := raft.NewNode(config1)
	node2 := raft.NewNode(config2)
	node3 := raft.NewNode(config3)

	// Start all nodes
	fmt.Println("📡 Starting all nodes...")
	node1.Start()
	node2.Start()
	node3.Start()

	defer func() {
		fmt.Println("🛑 Stopping all nodes...")
		node1.Stop()
		node2.Stop()
		node3.Stop()
	}()

	// Wait for initial election
	fmt.Println("⏳ Waiting for leader election...")
	time.Sleep(1 * time.Second)

	// Print cluster state
	printClusterState(node1, node2, node3)

	// Test proposing data from leader
	leader := findLeader(node1, node2, node3)
	if leader != nil {
		fmt.Printf("👑 Found leader: %s\n", leader.GetID())

		// Propose some data
		err := leader.Propose("test-data-1")
		if err != nil {
			fmt.Printf("❌ Failed to propose data: %v\n", err)
		} else {
			fmt.Println("✅ Successfully proposed data!")
		}
	} else {
		fmt.Println("❌ No leader found!")
	}

	fmt.Println("\n🎉 Test completed!")
}

func printClusterState(nodes ...*raft.Node) {
	for _, node := range nodes {
		state := "Follower"
		switch node.GetState() {
		case raft.NodeStateLeader:
			state = "👑 Leader"
		case raft.NodeStateCandidate:
			state = "🗳️ Candidate"
		}

		leader := node.GetLeader()
		if leader == "" {
			leader = "None"
		}

		fmt.Printf("  %s: %s (Term: %d, Leader: %s)\n",
			node.GetID(), state, node.GetTerm(), leader)
	}
}

func findLeader(nodes ...*raft.Node) *raft.Node {
	for _, node := range nodes {
		if node.GetState() == raft.NodeStateLeader {
			return node
		}
	}
	return nil
}
```

## Contributing

We welcome contributions to Xelection! Please feel free to submit issues, feature requests, or pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
