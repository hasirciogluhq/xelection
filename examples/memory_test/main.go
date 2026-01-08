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

	// Debug: Check votes for each candidate node
	fmt.Printf("📊 Vote counts:\n")
	for i, node := range []*raft.Node{node1, node2, node3} {
		if node.GetState() == raft.NodeStateCandidate {
			fmt.Printf("  node%d: Candidate in term %d\n", i+1, node.GetTerm())
		}
	}

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

		// Propose more data
		err = leader.Propose("test-data-2")
		if err != nil {
			fmt.Printf("❌ Failed to propose data: %v\n", err)
		} else {
			fmt.Println("✅ Successfully proposed second data!")
		}
	} else {
		fmt.Println("❌ No leader found!")
		fmt.Printf("Expected majority: %d votes\n", len([]raft.NodeID{"node1", "node2", "node3"})/2+1)
	}

	// Print final state
	fmt.Println("\n📊 Final cluster state:")
	printClusterState(node1, node2, node3)

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
