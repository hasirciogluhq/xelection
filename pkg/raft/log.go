package raft

// Propose proposes a new entry to be added to the log
func (n *Node) Propose(data interface{}) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != NodeStateLeader {
		return ErrNotLeader
	}

	// Create new log entry
	entry := LogEntry{
		Term:  n.term,
		Index: n.getLastLogIndex() + 1,
		Data:  data,
	}

	// Append to local log
	n.log = append(n.log, entry)

	// Replicate to followers
	go n.replicateLog()

	return nil
}

// replicateLog replicates log entries to all followers
func (n *Node) replicateLog() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != NodeStateLeader {
		return
	}

	for _, member := range n.clusterMembers {
		if member != n.id {
			go n.sendAppendEntries(member)
		}
	}
}

// sendAppendEntries sends append entries to a specific follower
func (n *Node) sendAppendEntries(follower NodeID) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Only send if we're still leader
	if n.state != NodeStateLeader {
		return
	}

	// TODO: Implement proper nextIndex tracking for each follower
	nextIndex := n.getLastLogIndex() + 1
	prevLogIndex := nextIndex - 1
	prevLogTerm := uint64(0)

	if prevLogIndex > 0 && len(n.log) >= int(prevLogIndex) {
		prevLogTerm = n.log[prevLogIndex-1].Term
	}

	// Get entries to send (empty for heartbeat)
	var entries []LogEntry
	if nextIndex <= n.getLastLogIndex() {
		entries = n.log[nextIndex-1:]
	}

	request := AppendEntriesRequest{
		Term:         n.term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}

	n.transport.Send(n.ctx, follower, Message{
		Type:    MessageTypeAppendEntriesRequest,
		Term:    n.term,
		From:    n.id,
		To:      follower,
		Payload: request,
	})
}

// getLastLogIndex returns the index of the last log entry
func (n *Node) getLastLogIndex() uint64 {
	if len(n.log) == 0 {
		return 0
	}
	return n.log[len(n.log)-1].Index
}

// Errors
var (
	ErrNotLeader = &raftError{"not a leader"}
)

type raftError struct {
	msg string
}

func (e *raftError) Error() string {
	return e.msg
}
