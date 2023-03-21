package dbtest

import (
	"context"
	"sort"
	"strings"

	"github.com/gogo/protobuf/proto"
	ragit "github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type Storage interface {
	ragit.StateMachine
	ragit.Storage
	Close()
}

type SMSuite struct {
	suite.Suite
	storage Storage
	create  func() Storage
}

func NewSMSuite(create func() Storage) *SMSuite {
	s := &SMSuite{
		create: create,
	}
	return s
}

func (s *SMSuite) SetupTest() {
	s.storage = s.create()
}

func (s *SMSuite) TearDownTest() {
	s.storage.Close()
	s.storage = nil
}

func (s *SMSuite) checkIndex(confIndex, dataIndex uint64) {
	state, err := s.storage.GetInitState()
	s.NoError(err)
	s.Equal(confIndex, state.ConfIndex)
	s.Equal(dataIndex, state.AppliedIndex)
}

func (s *SMSuite) TestApply() {

	state, err := s.storage.GetInitState()
	s.NoError(err)
	s.EqualValues(state.AppliedIndex, 1)
	s.EqualValues(state.ConfIndex, 1)
	s.EqualValues(state.LocalID, uint64(222))
	s.Len(state.ConfState.Voters, 3)

	hardState, confState, err := s.storage.InitialState()
	s.NoError(err)
	s.False(raft.IsEmptyHardState(hardState))
	s.Equal(state.ConfState, confState)

	s.checkIndex(1, 1)

	snapshot, err := s.storage.Snapshot()
	s.NoError(err)
	s.False(raft.IsEmptySnap(snapshot))

	opAdd := &refs.Oplog{
		Ops: []*refs.Oplog_Op{
			{
				Name:   proto.String("refs/heads/master"),
				Target: []byte("1234567890abcdef1234"),
			},
			{
				Name:   proto.String("refs/heads/branch2"),
				Target: []byte("1234567890abcdef1234"),
			},
		},
	}

	opUpdateRemove := &refs.Oplog{
		Ops: []*refs.Oplog_Op{
			{
				Name:      proto.String("refs/heads/master"),
				Target:    []byte("1234567890abcdef1235"),
				OldTarget: []byte("1234567890abcdef1234"),
			},
			{
				Name:      proto.String("refs/heads/branch2"),
				OldTarget: []byte("1234567890abcdef1234"),
			},
		},
	}

	s.NoError(s.storage.OnApply(2, 1, opAdd, context.Background()))

	var branch2, master refs.Hash
	copy(branch2[:], "1234567890abcdef1234")
	copy(master[:], "1234567890abcdef1234")

	refsMap := map[string]refs.Hash{
		"refs/heads/branch2": branch2,
		"refs/heads/master":  master,
	}

	members := []refs.Member{
		refs.NewMember(refs.PeerID(111), []string{"http://127.0.0.1:2022"}),
		refs.NewMember(refs.PeerID(222), []string{"http://127.0.0.2:2022"}),
		refs.NewMember(refs.PeerID(333), []string{"http://127.0.0.3:2022"}),
	}

	sData := &refs.SnapshotData{
		Refs:    refsMap,
		Members: members,
	}

	expectData, err := refs.EncodeSnapshot(sData)
	s.NoError(err)
	snapshot, err = s.storage.Snapshot()
	s.NoError(err)
	s.False(raft.IsEmptySnap(snapshot))
	s.Equal(uint64(2), snapshot.Metadata.Term)
	s.Equal(uint64(1), snapshot.Metadata.Index)
	s.Equal(string(expectData), string(snapshot.Data))
	s.checkIndex(1, 1)

	sDecodeData, err := refs.DecodeSnapshot(snapshot.Data)
	s.NoError(err)
	s.Equal(sDecodeData.Members[2].PeerURLs, []string{"http://127.0.0.3:2022"})
	s.Len(sDecodeData.Members, 3)
	s.EqualValues(sDecodeData.Refs["refs/heads/master"], master)

	s.NoError(s.storage.OnApply(3, 2, opUpdateRemove, context.Background()))

	refsMap = make(map[string]refs.Hash)
	copy(master[:], "1234567890abcdef1235")
	refsMap["refs/heads/master"] = master
	sData.Refs = refsMap

	expectData, err = refs.EncodeSnapshot(sData)
	s.NoError(err)
	snapshot, err = s.storage.Snapshot()
	s.NoError(err)
	s.False(raft.IsEmptySnap(snapshot))
	s.Equal(uint64(3), snapshot.Metadata.Term)
	s.Equal(uint64(2), snapshot.Metadata.Index)
	s.Equal(string(expectData), string(snapshot.Data))

	sDecodeData, err = refs.DecodeSnapshot(snapshot.Data)
	s.NoError(err)
	s.EqualValues(sDecodeData.Refs["refs/heads/master"], master)

	confState = pb.ConfState{
		Voters: []uint64{111, 222, 333, 444},
	}

	m := refs.NewMember(refs.PeerID(444), []string{"http://127.0.0.4:2022"})
	members = []refs.Member{
		m,
	}

	s.NoError(s.storage.OnConfState(3, confState, members, pb.ConfChangeAddNode))
	s.checkIndex(3, 2)

	memberURLs, err := s.storage.GetURLsByMemberID(refs.PeerID(111))
	s.NoError(err)
	s.Equal("http://127.0.0.1:2022", strings.Join(memberURLs, ","))

	confState = pb.ConfState{
		Voters: []uint64{111, 222, 333},
	}
	s.NoError(s.storage.OnConfState(4, confState, members, pb.ConfChangeRemoveNode))
	s.checkIndex(4, 2)
	_, err = s.storage.GetURLsByMemberID(refs.PeerID(444))
	s.Error(err)

	m3 := refs.NewMember(refs.PeerID(444), []string{"http://127.0.0.4:2022"})
	member3 := []refs.Member{
		m3,
	}

	confState = pb.ConfState{
		Voters:   []uint64{111, 222, 333},
		Learners: []uint64{444},
	}
	s.NoError(s.storage.OnConfState(5, confState, member3, pb.ConfChangeAddLearnerNode))
	s.checkIndex(5, 2)

	memberURLs, err = s.storage.GetURLsByMemberID(refs.PeerID(333))
	s.NoError(err)
	s.Equal("http://127.0.0.3:2022", strings.Join(memberURLs, ","))

	initState, err := s.storage.GetInitState()
	s.NoError(err)
	s.EqualValues(len(initState.Members), 4)

	_, confState, err = s.storage.InitialState()
	s.NoError(err)
	s.Equal([]uint64{111, 222, 333}, confState.Voters)
	s.Equal([]uint64{444}, confState.Learners)

	refsMap = make(map[string]refs.Hash)
	var branch1 refs.Hash
	copy(branch1[:], "3132333435363738393061626364656631323335")
	refsMap["refs/heads/branch1"] = branch1
	sData.Refs = refsMap
	sData.Members = append(sData.Members, refs.NewMember(refs.PeerID(444), []string{"http://127.0.0.4:2022"}))

	snapshot.Data, err = refs.EncodeSnapshot(sData)
	s.NoError(err)
	snapshot.Metadata.Term = 5
	snapshot.Metadata.Index = 10
	testObjSrcId := refs.PeerID(12345678)
	s.Error(s.storage.OnSnapshot(snapshot, testObjSrcId))

	members = []refs.Member{
		refs.NewMember(refs.PeerID(111), []string{"http://127.0.0.1:2022"}),
		refs.NewMember(refs.PeerID(222), []string{"http://127.0.0.2:2022"}),
		refs.NewMember(refs.PeerID(333), []string{"http://127.0.0.3:2022"}),
		refs.NewMember(refs.PeerID(444), []string{"http://127.0.0.4:2022"}),
	}
	sData.Members = members
	snapshot.Data, err = refs.EncodeSnapshot(sData)
	s.NoError(err)
	snapshot.Metadata.ConfState = confState
	s.NoError(s.storage.OnSnapshot(snapshot, refs.PeerID(111)))

	curSnap, err := s.storage.Snapshot()
	s.NoError(err)
	s.Equal(string(snapshot.Data), string(curSnap.Data))
	s.Equal(snapshot.Metadata, curSnap.Metadata)
	sDecodeData, err = refs.DecodeSnapshot(snapshot.Data)
	s.NoError(err)
	s.EqualValues(sDecodeData.Refs["refs/heads/branch1"], branch1)

	copy(branch2[:], "3132333435363738393061626364656631323334")
	sData.Refs["refs/heads/master"] = branch2
	sort.Slice(sData.Members, func(i, j int) bool {
		return sData.Members[i].ID < sData.Members[j].ID
	})
	snapshot.Data, err = refs.EncodeSnapshot(sData)
	s.NoError(err)
	snapshot.Metadata.Term = 5
	snapshot.Metadata.Index = 10
	s.NoError(s.storage.OnSnapshot(snapshot, refs.PeerID(111)))

	curSnap, err = s.storage.Snapshot()
	s.NoError(err)
	s.Equal(string(snapshot.Data), string(curSnap.Data))
	s.Equal(snapshot.Metadata, curSnap.Metadata)
	sDecodeData, err = refs.DecodeSnapshot(snapshot.Data)
	s.NoError(err)
	s.EqualValues(sDecodeData.Refs["refs/heads/master"], branch2)
}
