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

	member := refs.NewMember(123, []string{"http://127.0.0.1:2022"})
	mb, err := json.Marshal(member)
	s.NoError(err)
	cc.Context = mb

	members, err := getChangeMembers(cc)
	s.NoError(err)
	s.NotNil(members)
	s.Len(members, 1)
	for _, m := range members {
		s.EqualValues(m.ID, refs.PeerID(123))
		s.EqualValues(m.PeerURLs, []string{"http://127.0.0.1:2022"})
	}

	cc.Type = pb.ConfChangeAddLearnerNode
	members, err = getChangeMembers(cc)
	s.NoError(err)
	s.NotNil(members)
	s.Len(members, 1)
	for _, m := range members {
		s.EqualValues(m.ID, refs.PeerID(123))
		s.EqualValues(m.PeerURLs, []string{"http://127.0.0.1:2022"})
	}

	cc.Type = pb.ConfChangeUpdateNode
	members, err = getChangeMembers(cc)
	s.NoError(err)
	s.NotNil(members)
	s.Len(members, 1)
	for _, m := range members {
		s.Equal(m.ID, refs.PeerID(123))
		s.Equal(m.PeerURLs, []string{"http://127.0.0.1:2022"})
	}

	cc.Type = pb.ConfChangeRemoveNode
	cc.Context = nil
	members, err = getChangeMembers(cc)
	s.NoError(err)
	s.NotNil(members)
	s.Len(members, 1)
	for _, m := range members {
		s.EqualValues(m.ID, refs.PeerID(123))
		s.Nil(m.PeerURLs)
	}

	cc.Type = pb.ConfChangeRemoveNode
	cc.Context = make([]byte, 0)
	members, err = getChangeMembers(cc)
	s.NoError(err)
	s.NotNil(members)
	s.Len(members, 1)
	for _, m := range members {
		s.EqualValues(m.ID, refs.PeerID(123))
		s.Nil(m.PeerURLs)
	}
}
