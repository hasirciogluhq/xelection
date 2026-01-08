package raft

// Message represents a message sent between Raft nodes
type Message struct {
	Type    MessageType
	Term    uint64
	From    NodeID
	To      NodeID
	Payload interface{}
}

// MessageType defines the type of Raft message
type MessageType int

const (
	MessageTypeVoteRequest MessageType = iota
	MessageTypeVoteResponse
	MessageTypeAppendEntriesRequest
	MessageTypeAppendEntriesResponse
	MessageTypeInstallSnapshotRequest
	MessageTypeInstallSnapshotResponse
)
