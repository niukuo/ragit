package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

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

func (s Status) MemberStatus(
	getMemberURLs func() (map[PeerID][]string, error),
) (*MemberStatus, error) {

	// TODO: refactor member output
	memberURLs, err := getMemberURLs()
	if err != nil {
		return nil, err
	}

	var leaderURLs []string
	if s.Lead != 0 {
		leaderURLs = memberURLs[PeerID(s.Lead)]
	}

	idURLs := memberURLs[PeerID(s.ID)]

	mstatus := &MemberStatus{
		ID:        strings.Join(idURLs, ","),
		Lead:      strings.Join(leaderURLs, ","),
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

		us, ok := memberURLs[PeerID(k)]
		if !ok {
			return nil, fmt.Errorf("id: %s not found in members: %v",
				PeerID(k), memberURLs)
		}
		mstatus.Progress[strings.Join(us, ",")] = tracker
		mstatus.Members[PeerID(k).String()] = us
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
