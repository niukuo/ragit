package raft

import "fmt"

type errTermChanged struct {
	expTerm uint64
	curTerm uint64
}

func (e *errTermChanged) Error() string {
	if e.expTerm != 0 {
		return fmt.Sprintf("term changed, exp_term: %d, cur_term: %d", e.expTerm, e.curTerm)
	}
	return fmt.Sprintf("term changed, cur_term: %v", e.curTerm)
}
