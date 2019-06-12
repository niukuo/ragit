package raft

import "fmt"

type errTermChanged struct {
	proposedTerm  uint64
	committedTerm uint64
}

func (e *errTermChanged) Error() string {
	return fmt.Sprintf("term changed, proposed: %v, committed: %v",
		e.proposedTerm, e.committedTerm)
}
