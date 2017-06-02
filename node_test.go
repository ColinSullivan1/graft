// Copyright 2013-2016 Apcera Inc. All rights reserved.

package graft

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	// Test bad ClusterInfos
	bci := ClusterInfo{Name: "", Size: 5}
	if _, err := New(bci, nil, nil, "log"); err == nil || err != ClusterNameErr {
		t.Fatal("Expected an error with empty cluster name")
	}
	bci = ClusterInfo{Name: "foo", Size: 0}
	if _, err := New(bci, nil, nil, "log"); err == nil || err != ClusterSizeErr {
		t.Fatal("Expected an error with invalid cluster size")
	}

	// Good ClusterInfo
	ci := ClusterInfo{Name: "foo", Size: 3}

	// Handler is required
	if _, err := New(ci, nil, nil, "log"); err == nil || err != HandlerReqErr {
		t.Fatal("Expected an error with no handler argument")
	}

	hand, rpc, log := genNodeArgs(t)

	// rpcDriver is required
	if _, err := New(ci, hand, nil, "log"); err == nil || err != RpcDriverReqErr {
		t.Fatal("Expected an error with no rpcDriver argument")
	}

	// Test if rpc Init fails we get error from New()
	badRpc := &MockRpcDriver{shouldFailInit: true}
	if _, err := New(ci, hand, badRpc, "log"); err == nil {
		t.Fatal("Expected an error with a bad rpcDriver argument")
	}

	// Test peer count
	mpc := mockPeerCount()
	if mpc != 0 {
		t.Fatalf("Incorrect peer count, expected 0 got %d\n", mpc)
	}

	// log is required
	if _, err := New(ci, hand, rpc, ""); err == nil || err != LogReqErr {
		t.Fatal("Expected an error with no log argument")
	}

	node, err := New(ci, hand, rpc, log)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	defer node.Close()

	// Check default state
	if state := node.State(); state != FOLLOWER {
		t.Fatalf("Expected new Node to be in Follower state, got: %s", state)
	}
	// Check string version of state
	if stateStr := node.State().String(); stateStr != "Follower" {
		t.Fatalf("Expected new Node to be in Follower state, got: %s", stateStr)
	}

	if node.Leader() != NO_LEADER {
		t.Fatalf("Expected no leader to start, got: %s\n", node.Leader())
	}
	if node.CurrentTerm() != 0 {
		t.Fatalf("Expected CurrentTerm of 0, got: %d\n", node.CurrentTerm())
	}
}

func TestClose(t *testing.T) {
	base := runtime.NumGoroutine()

	ci := ClusterInfo{Name: "foo", Size: 3}
	hand, rpc, log := genNodeArgs(t)

	node, err := New(ci, hand, rpc, log)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	node.Close()

	if node.isRunning() {
		t.Fatal("Expected isRunning() to return false")
	}

	// Check state
	if state := node.State(); state != CLOSED {
		t.Fatalf("Expected node to be in Closed state, got: %s", state)
	}
	if stateStr := node.State().String(); stateStr != "Closed" {
		t.Fatalf("Expected node to be in Closed state, got: %s", stateStr)
	}

	// Check to make sure rpc.Close() was called.
	if rawRpc := rpc.(*MockRpcDriver); !rawRpc.closeCalled {
		t.Fatalf("RPCDriver was not shutdown properly")
	}

	// Make sure the timers were cleared.
	if node.electTimer != nil {
		t.Fatalf("electTimer was not cleared")
	}

	// Check for dangling go routines
	delta := (runtime.NumGoroutine() - base)
	if delta > 0 {
		t.Fatalf("[%d] Go routines still exist post Close()", delta)
	}
}

func TestElectionTimeoutDuration(t *testing.T) {
	et := randElectionTimeout()
	if et < MIN_ELECTION_TIMEOUT || et > MAX_ELECTION_TIMEOUT {
		t.Fatalf("Election Timeout expected to be between %d-%d ms, got %d ms",
			MIN_ELECTION_TIMEOUT/time.Millisecond,
			MAX_ELECTION_TIMEOUT/time.Millisecond,
			et/time.Millisecond)
	}
}

func TestCandidateState(t *testing.T) {
	ci := ClusterInfo{Name: "foo", Size: 3}
	hand, rpc, log := genNodeArgs(t)
	node, err := New(ci, hand, rpc, log)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	defer node.Close()

	// Should move to candidate state within MAX_ELECTION_TIMEOUT
	if state := waitForState(node, CANDIDATE); state != CANDIDATE {
		t.Fatalf("Expected node to move to Candidate state, got: %s", state)
	}
	if stateStr := node.State().String(); stateStr != "Candidate" {
		t.Fatalf("Expected node to move to Candidate state, got: %s", stateStr)
	}
}

func TestLeaderState(t *testing.T) {
	// Expected of 1, we should immediately win the election.
	ci := ClusterInfo{Name: "foo", Size: 1}
	hand, rpc, log := genNodeArgs(t)
	node, err := New(ci, hand, rpc, log)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	defer node.Close()

	// Should move to leader state within MAX_ELECTION_TIMEOUT
	if state := waitForState(node, LEADER); state != LEADER {
		t.Fatalf("Expected node to move to Leader state, got: %s", state)
	}
	if stateStr := node.State().String(); stateStr != "Leader" {
		t.Fatalf("Expected node to move to Leader state, got: %s", stateStr)
	}
}

func TestSimpleLeaderElection(t *testing.T) {
	toStart := 5
	nodes := createNodes(t, "foo", toStart)
	// Do cleanup
	for _, n := range nodes {
		defer n.Close()
	}

	expectedClusterState(t, nodes, 1, toStart-1, 0)
}

func TestStaggeredStart(t *testing.T) {
	ci := ClusterInfo{Name: "staggered", Size: 3}
	nodes := make([]*Node, 3)
	for i := 0; i < 3; i++ {
		hand, rpc, logPath := genNodeArgs(t)
		node, err := New(ci, hand, rpc, logPath)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		nodes[i] = node
		time.Sleep(MAX_ELECTION_TIMEOUT)
	}
	// Do cleanup
	for _, n := range nodes {
		defer n.Close()
	}
	expectedClusterState(t, nodes, 1, 2, 0)
}

func TestDownToOneAndBack(t *testing.T) {
	nodes := createNodes(t, "downtoone", 3)
	expectedClusterState(t, nodes, 1, 2, 0)

	// Do cleanup
	for _, n := range nodes {
		defer n.Close()
	}

	// find and kill the leader
	leader := findLeader(nodes)
	leader.Close()
	expectedClusterState(t, nodes, 1, 1, 0)

	// start a new process in the leader's place
	leader = findLeader(nodes)
	follower := firstFollower(nodes)
	nodes = []*Node{leader, follower}
	hand, rpc, logPath := genNodeArgs(t)
	newNode, err := New(leader.ClusterInfo(), hand, rpc, logPath)
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	defer newNode.Close()
	nodes = append(nodes, newNode)
	expectedClusterState(t, nodes, 1, 2, 0)

	// find and kill the new leader
	leader = findLeader(nodes)
	leader.Close()
	expectedClusterState(t, nodes, 1, 1, 0)

	// find the leader again and kill it
	leader = findLeader(nodes)
	leader.Close()
	expectedClusterState(t, nodes, 0, 0, 1)

	// grab the surviving node, we'll want to compare term numbers
	var survivingNode *Node
	for _, n := range nodes {
		if n.State() == CANDIDATE {
			survivingNode = n
			break
		}
	}
	if survivingNode == nil {
		t.Fatal("Failed to find the surving node")
	}

	// start the two other nodes back up
	nodes = []*Node{survivingNode}
	for i := 0; i < 2; i++ {
		hand, rpc, logPath := genNodeArgs(t)
		node, err := New(survivingNode.ClusterInfo(), hand, rpc, logPath)
		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
		nodes = append(nodes, node)
		defer node.Close()
	}

	// we expect to be in a consistent state and for terms to match
	expectedClusterState(t, nodes, 1, 2, 0)
	leader = findLeader(nodes)
	if leader.CurrentTerm() != survivingNode.CurrentTerm() {
		t.Fatalf("term between leader and survivor didn't match. leader: %d, survivor: %d", leader.CurrentTerm(), survivingNode.CurrentTerm())
	}
}

func TestReElection(t *testing.T) {
	toStart := 5
	nodes := createNodes(t, "foo", toStart)
	// Do cleanup
	for _, n := range nodes {
		defer n.Close()
	}

	time.Sleep(MAX_ELECTION_TIMEOUT)

	// Find and close down the leader
	leader := findLeader(nodes)
	if leader == nil {
		t.Fatal("Could not find a leader!\n")
	}
	leader.Close()

	// Make sure we have another leader.
	expectedClusterState(t, nodes, 1, toStart-2, 0)
}

func TestNetworkSplit(t *testing.T) {
	clusterSize := 5

	nodes := createNodes(t, "foo", clusterSize)
	// Do cleanup
	for _, n := range nodes {
		defer n.Close()
	}

	// Make sure we have correct count.
	expectedClusterState(t, nodes, 1, clusterSize-1, 0)

	// Simulate a network split. We will pick the leader and 1 follower
	// to be in one group, all others will be in the other.

	theLeader := findLeader(nodes)
	if theLeader == nil {
		t.Fatal("Expected to find a leader, got <nil>")
	}
	aFollower := firstFollower(nodes)
	if aFollower == nil {
		t.Fatal("Expected to find a follower, got <nil>")
	}
	grp := []*Node{theLeader, aFollower}

	// Split the nodes in two..
	mockSplitNetwork(grp)

	// Make sure we have another leader.
	expectedClusterState(t, nodes, 2, clusterSize-2, 0)

	// Restore Communications
	mockRestoreNetwork()

	expectedClusterState(t, nodes, 1, clusterSize-1, 0)
}

type TestLog struct {
	mu        sync.Mutex
	T         testing.T
	Term      uint64
	VotedFor  string
	LastIndex uint64
	LastEntry []byte
	Closed    bool
}

// NewDefaultLog creates a default log that persists term and last
// voted for.  Path is required to be valid.
func NewTestLog(t *testing.T, lastIndex uint64, lastEntry []byte) *TestLog {
	return &TestLog{
		LastIndex: lastIndex,
		LastEntry: lastEntry,
	}
}

// Close cleans up the log and frees system resources.
func (l *TestLog) Close() error {
	l.Closed = true
	return nil
}

// LatestEntry returns the last term and last voted for.  History is always
// nil in this implementation.
func (l *TestLog) LatestEntry() (uint64, string, uint64, []byte, error) {
	return l.Term, l.VotedFor, l.LastIndex, l.LastEntry, nil
}

// AppendEntry saves the term and voted for values to the log.  Only one entry is
// held in this implementation.
func (l *TestLog) AppendEntry(term uint64, votedFor string, index uint64, entry []byte) error {
	l.Term = term
	l.VotedFor = votedFor

	// ignore index and entry, they are set elsewhere.
	return nil
}

// LogUpToDate compares two log indexes (local and the candidate)
// It returns true if the candidate log is up to date, false otherwise.
func (l *TestLog) LogUpToDate(index uint64, info []byte, candidateIndex uint64, candidateInfo []byte) bool {
	fmt.Printf("Comparing index %d to candidateIndex %d\n", index, candidateIndex)
	if index == candidateIndex {
		return true
		/*le, err := strconv.Atoi(string(localInfo))
		if err != nil {
			panic("unable to convert local log entry")
		}
		ce, err := strconv.Atoi(string(candidateInfo))
		if err != nil {
			panic("unable to convert local log entry")
		}
		if le == ce {
			return 0
		}
		if le > ce {
			return 1
		}
		return -1*/
	}

	return index < candidateIndex
}

func createHistoryNode(t *testing.T, ci *ClusterInfo, logIndex uint64, entry string) *Node {
	hand, rpc, _ := genNodeArgs(t)
	n, err := NewWithLog(*ci, hand, rpc, NewTestLog(t, logIndex, []byte(entry)))
	if err != nil {
		t.Fatalf("error creating node with history: %v", err)
	}
	return n
}

func TestLeaderFromHistory(t *testing.T) {
	numNodes := 15
	name := "test-cluster"
	ci := ClusterInfo{Name: name, Size: numNodes}

	nodes := make([]*Node, 0)
	nodes = append(nodes, createHistoryNode(t, &ci, 99, ""))
	for i := 0; i < numNodes-1; i++ {
		nodes = append(nodes, createHistoryNode(t, &ci, uint64(i), "1"))
	}

	for _, n := range nodes {
		defer n.Close()
	}

	expectedClusterState(t, nodes, 1, numNodes-1, 0)

	leader := findLeader(nodes)
	if leader.LastIndex() != 99 {
		t.Fatalf("incorrect leader - index = %d", leader.LastIndex())
	}
}
