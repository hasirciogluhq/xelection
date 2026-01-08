package raft

// VoteRequest represents a vote request message
type VoteRequest struct {
	Term         uint64
	CandidateID  NodeID
	LastLogIndex uint64
	LastLogTerm  uint64
}

// VoteResponse represents a vote response message
type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

// AppendEntriesRequest represents an append entries request message
type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     NodeID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

// AppendEntriesResponse represents an append entries response message
type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

// LogEntry represents a log entry in the Raft log
type LogEntry struct {
	Term  uint64
	Index uint64
	Data  interface{}
}
