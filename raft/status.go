package raft

import (
	"encoding/json"
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

func (s Status) MemberStatus() *MemberStatus {
	mstatus := &MemberStatus{
		ID:        PeerID(s.ID).String(),
		Lead:      PeerID(s.Lead).String(),
		RaftState: s.RaftState.String(),
	}
	mstatus.Progress = make(map[string]Tracker, 0)

	for k, v := range s.Progress {
		tracker := Tracker{
			Match: v.Match,
			Next:  v.Next,
			State: v.State.String(),
		}
		id := PeerID(k).String()
		mstatus.Progress[id] = tracker
	}

	return mstatus
}

type Tracker struct {
	Match uint64 `json:"match"`
	Next  uint64 `json:"next"`
	State string `json:"state"`
}

type MemberStatus struct {
	ID        string             `json:"id"`
	Lead      string             `json:"lead"`
	RaftState string             `json:"raftState"`
	Progress  map[string]Tracker `json:"progress"`
}

func (s MemberStatus) String() string {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}
