// Copyright 2013-2017 Apcera Inc. All rights reserved.

package graft

import (
	"bytes"
	"crypto/sha1"
	"encoding/json"
	"io/ioutil"
	"os"
)

type envelope struct {
	SHA, Data []byte
}

type persistentState struct {
	CurrentTerm uint64
	VotedFor    string
}

// DefaultLog is a log desiged for leadership election only;
// only term and last voted for is persisted.  Comparison of
// log history is a NOOP.
type DefaultLog struct {
	path string
}

// NewDefaultLog creates a default log that persists term and last
// voted for.  Path is required to be valid.
func NewDefaultLog(path string) *DefaultLog {
	return &DefaultLog{
		path: path,
	}
}

// Close cleans up the log and frees system resources.
func (l *DefaultLog) Close() error {
	err := os.Remove(l.path)
	l.path = ""
	return err
}

// LatestEntry returns the last term and last voted for.  History is always
// nil in this implementation.
func (l *DefaultLog) LatestEntry() (uint64, string, uint64, []byte, error) {
	buf, err := ioutil.ReadFile(l.path)
	if err != nil {
		return 0, "", 0, nil, err
	}
	if len(buf) <= 0 {
		return 0, "", 0, nil, LogNoStateErr
	}

	env := &envelope{}
	if err := json.Unmarshal(buf, env); err != nil {
		return 0, "", 0, nil, err
	}

	// Test for corruption
	sha := sha1.New().Sum(env.Data)
	if !bytes.Equal(sha, env.SHA) {
		return 0, "", 0, nil, LogCorruptErr
	}

	ps := &persistentState{}
	if err := json.Unmarshal(env.Data, ps); err != nil {
		return 0, "", 0, nil, err
	}

	return ps.CurrentTerm, ps.VotedFor, 0, nil, nil
}

// AppendEntry saves the term and voted for values to the log.  Only one entry is
// held in this implementation.
func (l *DefaultLog) AppendEntry(term uint64, votedFor string, index uint64, entry []byte) error {
	ps := persistentState{
		CurrentTerm: term,
		VotedFor:    votedFor,
	}
	logPath := l.path

	buf, err := json.Marshal(ps)
	if err != nil {
		return err
	}

	// Set a SHA1 to test for corruption on read
	env := envelope{
		SHA:  sha1.New().Sum(buf),
		Data: buf,
	}

	toWrite, err := json.Marshal(env)
	if err != nil {
		return err
	}

	return ioutil.WriteFile(logPath, toWrite, 0660)
}

// LogUpToDate compares two log indexes and last entries.
// Returns a postive number if the first log is newer, a negative number
// if older, and zero on equality.
func (l *DefaultLog) LogUpToDate(index uint64, info []byte, candidateIndex uint64, candidateInfo []byte) bool {
	// the default log is for leadership election only and thus
	// has no history (only).
	return true
}
