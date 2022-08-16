package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/niukuo/ragit/refs"
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
			subj := fmt.Sprintf(`"%s":{"match":%d,"next":%d,"state":%q,"isLearner":%t},`, PeerID(k), v.Match, v.Next, v.State, v.IsLearner)
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

func (s Status) MemberStatus(getMemberAddrs func(memberID refs.PeerID) ([]string, error)) (*MemberStatus, error) {
	idStrs, err := getMemberAddrs(PeerID(s.ID))
	if err != nil {
		return nil, err
	}

	var leadStrs []string
	if s.Lead != 0 {
		leadStrs, err = getMemberAddrs(PeerID(s.Lead))
		if err != nil {
			return nil, err
		}
	}

	mstatus := &MemberStatus{
		ID:        strings.Join(idStrs, ","),
		Lead:      strings.Join(leadStrs, ","),
		Commit:    s.Commit,
		RaftState: s.RaftState.String(),
	}
	mstatus.Progress = make(map[string]Tracker, 0)
	mstatus.Members = make(map[string][]string)

	for k, v := range s.Progress {
		tracker := Tracker{
			Match:     v.Match,
			Next:      v.Next,
			State:     v.State.String(),
			IsLearner: v.IsLearner,
		}
		addrs, err := getMemberAddrs(PeerID(k))
		if err != nil {
			return nil, err
		}
		mstatus.Progress[strings.Join(addrs, ",")] = tracker
		mstatus.Members[PeerID(k).String()] = addrs
	}

	return mstatus, nil
}

type Tracker struct {
	Match     uint64 `json:"match"`
	Next      uint64 `json:"next"`
	State     string `json:"state"`
	IsLearner bool   `json:"isLearner"`
}

type MemberStatus struct {
	ID        string              `json:"id"`
	Lead      string              `json:"lead"`
	Commit    uint64              `json:"commit"`
	RaftState string              `json:"raftState"`
	Progress  map[string]Tracker  `json:"progress"`
	Members   map[string][]string `json:"members"`
}

func (s MemberStatus) String() string {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err.Error()
	}
	return string(data)
}
