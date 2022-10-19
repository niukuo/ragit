package raft

import (
	"encoding/json"
	"testing"

	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/assert"
	pb "go.etcd.io/etcd/raft/raftpb"
)

func TestCheckAndGetChangeMembers(t *testing.T) {
	s := assert.New(t)

	mockStore := &MockStorage{}
	rh := &readyHandler{
		storage: mockStore,
	}

	// add learner
	mockStore.On("GetAllMemberURLs").Return(map[PeerID][]string{}, nil).Once()
	cc := pb.ConfChange{
		Type:   pb.ConfChangeAddLearnerNode,
		NodeID: uint64(123),
	}
	confChangeContext := ConfChangeContext{
		Member:    refs.NewMember(123, []string{"http://127.0.0.1:2022"}),
		IsPromote: false,
	}
	ctxb, err := json.Marshal(confChangeContext)
	s.NoError(err)
	cc.Context = ctxb

	members, err := rh.checkAndGetChangeMembers(cc)
	s.NoError(err)
	s.Len(members, 1)
	s.Equal(refs.PeerID(123), members[0].ID)
	s.Equal([]string{"http://127.0.0.1:2022"}, members[0].PeerURLs)

	mockStore.On("GetAllMemberURLs").Return(map[PeerID][]string{
		PeerID(123): {"http://127.0.0.1:2022"},
	}, nil).Once()
	_, err = rh.checkAndGetChangeMembers(cc)
	s.Error(err)

	// promote
	mockStore.On("GetAllMemberURLs").Return(map[PeerID][]string{
		PeerID(123): {"http://127.0.0.1:2022"},
	}, nil).Twice()

	mockStore.On("GetConfState").Return(&pb.ConfState{
		Learners: []uint64{},
	}, nil).Once()

	confChangeContext.IsPromote = true
	ctxWithPromote, err := json.Marshal(confChangeContext)
	s.NoError(err)

	cc = pb.ConfChange{
		Type:    pb.ConfChangeAddNode,
		NodeID:  uint64(123),
		Context: ctxWithPromote,
	}

	_, err = rh.checkAndGetChangeMembers(cc)
	s.Error(err)

	mockStore.On("GetConfState").Return(&pb.ConfState{
		Learners: []uint64{123},
	}, nil).Once()

	cc = pb.ConfChange{
		Type:    pb.ConfChangeAddNode,
		NodeID:  uint64(123),
		Context: ctxWithPromote,
	}

	members, err = rh.checkAndGetChangeMembers(cc)
	s.NoError(err)
	s.Len(members, 0)

	// add
	mockStore.On("GetAllMemberURLs").Return(map[PeerID][]string{}, nil).Once()
	cc = pb.ConfChange{
		Type:    pb.ConfChangeAddNode,
		NodeID:  uint64(123),
		Context: ctxb,
	}

	members, err = rh.checkAndGetChangeMembers(cc)
	s.NoError(err)
	s.Len(members, 1)
	s.Equal(refs.PeerID(123), members[0].ID)
	s.Equal([]string{"http://127.0.0.1:2022"}, members[0].PeerURLs)

	mockStore.On("GetAllMemberURLs").Return(map[PeerID][]string{
		PeerID(123): {"http://127.0.0.1:2022"},
	}, nil).Once()
	_, err = rh.checkAndGetChangeMembers(cc)
	s.Error(err)

	// update
	mockStore.On("GetAllMemberURLs").Return(map[PeerID][]string{}, nil).Once()
	cc = pb.ConfChange{
		Type:    pb.ConfChangeUpdateNode,
		NodeID:  uint64(123),
		Context: ctxb,
	}

	_, err = rh.checkAndGetChangeMembers(cc)
	s.Error(err)

	mockStore.On("GetAllMemberURLs").Return(map[PeerID][]string{
		PeerID(123): {"http://127.0.0.1:2021"},
	}, nil).Once()
	members, err = rh.checkAndGetChangeMembers(cc)
	s.NoError(err)
	s.Len(members, 1)
	s.Equal([]string{"http://127.0.0.1:2022"}, members[0].PeerURLs)

	// remove
	mockStore.On("GetAllMemberURLs").Return(map[PeerID][]string{
		PeerID(123): {"http://127.0.0.1:2022"},
	}, nil).Twice()

	cc = pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode,
		NodeID: uint64(123),
	}
	members, err = rh.checkAndGetChangeMembers(cc)
	s.NoError(err)
	s.Len(members, 1)
	s.Equal(refs.PeerID(123), members[0].ID)
	s.Nil(members[0].PeerURLs)

	cc = pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode,
		NodeID: uint64(124),
	}
	_, err = rh.checkAndGetChangeMembers(cc)
	s.Error(err)
}
