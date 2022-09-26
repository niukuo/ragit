package raft

import (
	"encoding/json"
	"testing"

	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/assert"
	pb "go.etcd.io/etcd/raft/raftpb"
)

func TestGetChangeMembers(t *testing.T) {
	s := assert.New(t)

	cc := pb.ConfChange{
		Type:   pb.ConfChangeAddNode,
		NodeID: uint64(123),
	}

	member1 := refs.NewMember(123, []string{"http://127.0.0.1:2022"})
	mb, err := json.Marshal(member1)
	s.NoError(err)
	cc.Context = mb

	m1, err := getChangeMembers(cc)
	s.NoError(err)
	s.NotNil(m1)
	s.Len(m1, 1)
	for _, m := range m1 {
		s.EqualValues(m.ID, refs.PeerID(123))
		s.EqualValues(m.PeerURLs, []string{"http://127.0.0.1:2022"})
	}

	cc.Type = pb.ConfChangeAddLearnerNode
	m2, err := getChangeMembers(cc)
	s.NoError(err)
	s.NotNil(m2)
	s.Len(m2, 1)
	for _, m := range m2 {
		s.EqualValues(m.ID, refs.PeerID(123))
		s.EqualValues(m.PeerURLs, []string{"http://127.0.0.1:2022"})
	}

	cc.Type = pb.ConfChangeUpdateNode
	m3, err := getChangeMembers(cc)
	s.NoError(err)
	s.NotNil(m3)
	s.Len(m3, 1)
	for _, m := range m3 {
		s.EqualValues(m.ID, refs.PeerID(123))
		s.EqualValues(m.PeerURLs, []string{"http://127.0.0.1:2022"})
	}

	cc.Type = pb.ConfChangeRemoveNode
	cc.Context = nil
	m4, err := getChangeMembers(cc)
	s.NoError(err)
	s.NotNil(m4)
	s.Len(m4, 1)
	for _, m := range m4 {
		s.EqualValues(m.ID, refs.PeerID(123))
		s.Nil(m.PeerURLs)
	}

	cc.Type = pb.ConfChangeRemoveNode
	cc.Context = make([]byte, 0)
	m5, err := getChangeMembers(cc)
	s.NoError(err)
	s.NotNil(m5)
	s.Len(m5, 1)
	for _, m := range m5 {
		s.EqualValues(m.ID, refs.PeerID(123))
		s.Nil(m.PeerURLs)
	}
}
