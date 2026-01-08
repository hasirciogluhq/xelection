package raft

import (
	"fmt"
	"time"
)

// PartitionID uniquely identifies a partition
type PartitionID string

// PartitionNode represents a node that can participate in partition-level elections
type PartitionNode struct {
	*Node

	// Partition-specific state
	partitions map[PartitionID]*PartitionState

	// Partition configuration
	partitionConfig map[PartitionID]PartitionConfig
}

// PartitionState holds state for a specific partition
type PartitionState struct {
	ID            PartitionID
	Role          PartitionRole
	Leader        NodeID
	Term          uint64
	VotedFor      NodeID
	LastHeartbeat time.Time
}

// PartitionRole represents the role of a node in a partition
type PartitionRole int

const (
	PartitionRoleFollower PartitionRole = iota
	PartitionRoleCandidate
	PartitionRoleLeader
)

// PartitionConfig holds configuration for a partition
type PartitionConfig struct {
	ID              PartitionID
	Members         []NodeID
	ElectionTimeout time.Duration
}

// NewPartitionNode creates a new partition-aware Raft node
func NewPartitionNode(config Config, partitionConfigs []PartitionConfig) *PartitionNode {
	node := NewNode(config)

	pNode := &PartitionNode{
		Node:            node,
		partitions:      make(map[PartitionID]*PartitionState),
		partitionConfig: make(map[PartitionID]PartitionConfig),
	}

	// Initialize partition states
	for _, pConfig := range partitionConfigs {
		pNode.partitionConfig[pConfig.ID] = pConfig
		pNode.partitions[pConfig.ID] = &PartitionState{
			ID:            pConfig.ID,
			Role:          PartitionRoleFollower,
			Leader:        "",
			Term:          0,
			VotedFor:      "",
			LastHeartbeat: time.Now(),
		}
	}

	return pNode
}

// GetPartitionRole returns the role of the node in a specific partition
func (pn *PartitionNode) GetPartitionRole(partitionID PartitionID) PartitionRole {
	pn.mu.RLock()
	defer pn.mu.RUnlock()

	if state, exists := pn.partitions[partitionID]; exists {
		return state.Role
	}
	return PartitionRoleFollower
}

// GetPartitionLeader returns the leader of a specific partition
func (pn *PartitionNode) GetPartitionLeader(partitionID PartitionID) NodeID {
	pn.mu.RLock()
	defer pn.mu.RUnlock()

	if state, exists := pn.partitions[partitionID]; exists {
		return state.Leader
	}
	return ""
}

// ProposeToPartition proposes a command to a specific partition
func (pn *PartitionNode) ProposeToPartition(partitionID PartitionID, data interface{}) error {
	pn.mu.RLock()
	defer pn.mu.RUnlock()

	state, exists := pn.partitions[partitionID]
	if !exists {
		return fmt.Errorf("partition %s not found", partitionID)
	}

	if state.Role != PartitionRoleLeader {
		return ErrNotPartitionLeader
	}

	// TODO: Implement partition-specific log replication
	return nil
}

// Errors
var (
	ErrNotPartitionLeader = &raftError{"not a partition leader"}
)
