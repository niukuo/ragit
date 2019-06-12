package raft

import (
	"fmt"
	"log"

	"go.etcd.io/etcd/raft"
)

type Status raft.Status

func (s Status) MarshalJSON() ([]byte, error) {
	j := fmt.Sprintf(`{"id":"%s","term":%d,"vote":"%s","commit":%d,"lead":"%s","raftState":%q,"applied":%d,"progress":{`,
		PeerID(s.ID), s.Term, PeerID(s.Vote), s.Commit, PeerID(s.Lead), s.RaftState, s.Applied)

	if len(s.Progress) == 0 {
		j += "},"
	} else {
		for k, v := range s.Progress {
			subj := fmt.Sprintf(`"%s":{"match":%d,"next":%d,"state":%q},`, PeerID(k), v.Match, v.Next, v.State)
			j += subj
		}
		// remove the trailing ","
		j = j[:len(j)-1] + "},"
	}

	j += fmt.Sprintf(`"leadtransferee":"%x"}`, s.LeadTransferee)
	return []byte(j), nil
}

func (s Status) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		log.Panicf("unexpected error: %v", err)
	}
	return string(b)
}
