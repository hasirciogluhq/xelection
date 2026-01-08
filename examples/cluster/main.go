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
