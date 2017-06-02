// Copyright 2013-2016 Apcera Inc. All rights reserved.

package graft

// Log allows users to override the log implementation of GRAFT, specifically
// to allow the retrieval and comparison of history (log entries).
type Log interface {

	// LatestEntry returns the term, last voted for, last index and latest entry in the log
	LatestEntry() (term uint64, votedFor string, lastIndex uint64, logInfo []byte, err error)

	// LogUpToDate returns true if the candidate log is up to date, false otherwise.
	LogUpToDate(index uint64, info []byte, candidateIndex uint64, candidateInfo []byte) bool

	// AppendEntry appends to the log (or sets the last entry)
	// TODO:  This implementation does not pass an index and entry, they should be ignored and obtained elsewhere
	// by the Log implementor?  Leave for future?
	AppendEntry(term uint64, votedFor string, index uint64, entry []byte) error

	// Closes the log, freeing resources
	Close() error
}

func (n *Node) closeLog() error {
	err := n.log.Close()
	n.logPath = ""
	return err
}

// writeState writes the current state.  Note that lastIndex and lastEntry will not
// change here in this implementation, so they are not passed.
func (n *Node) writeState() error {
	return n.log.AppendEntry(n.term, n.vote, 0, nil)
}

func (n *Node) readState() error {
	currentTerm, lastVoted, lastIndex, logInfo, err := n.log.LatestEntry()
	if err != nil && err != LogNoStateErr {
		return err
	}

	// Update our state from the log
	n.setTerm(currentTerm)
	n.setVote(lastVoted)
	n.SetLastIndex(lastIndex)
	n.SetLogInfo(logInfo)

	return nil
}
