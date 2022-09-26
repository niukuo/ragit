package dbtest

import (
	"context"
	"sort"
	"strings"

	"github.com/gogo/protobuf/proto"
	ragit "github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
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

	opAdd := refs.Oplog{
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

	opUpdateRemove := refs.Oplog{
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

	refsMap := make(map[string]refs.Hash)
	membersSlice := make([]refs.Member, 3)
	membersSlice[0] = refs.NewMember(refs.PeerID(111), []string{"127.0.0.1:2022"})
	membersSlice[1] = refs.NewMember(refs.PeerID(222), []string{"127.0.0.2:2022"})
	membersSlice[2] = refs.NewMember(refs.PeerID(333), []string{"127.0.0.3:2022"})
	var refhashBranch2 refs.Hash
	var valBranch2 []byte = []byte("1234567890abcdef1234")
	copy(refhashBranch2[:], valBranch2)
	refsMap["refs/heads/branch2"] = refhashBranch2
	var refhashMaster refs.Hash
	var valBranchMaster []byte = []byte("1234567890abcdef1234")
	copy(refhashMaster[:], valBranchMaster)
	refsMap["refs/heads/master"] = refhashMaster

	sData := &refs.SnapshotData{
		Refs:    refsMap,
		Members: membersSlice,
	}

	membersHttp := make([]refs.Member, 3)
	membersHttp[0] = refs.NewMember(refs.PeerID(111), []string{"http://127.0.0.1:2022"})
	membersHttp[1] = refs.NewMember(refs.PeerID(222), []string{"http://127.0.0.2:2022"})
	membersHttp[2] = refs.NewMember(refs.PeerID(333), []string{"http://127.0.0.3:2022"})
	sData.Members = membersHttp
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
	s.EqualValues(sDecodeData.Members[2].PeerURLs, []string{"http://127.0.0.3:2022"})
	s.EqualValues(len(sDecodeData.Members), 3)
	s.EqualValues(sDecodeData.Refs["refs/heads/master"], refhashMaster)

	s.NoError(s.storage.OnApply(3, 2, opUpdateRemove, context.Background()))

	refsMap2 := make(map[string]refs.Hash)
	var valBranchMaster2 []byte = []byte("1234567890abcdef1235")
	var refhashMaster2 refs.Hash
	copy(refhashMaster2[:], valBranchMaster2)
	refsMap2["refs/heads/master"] = refhashMaster2
	sData.Refs = refsMap2
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
	s.EqualValues(sDecodeData.Refs["refs/heads/master"], refhashMaster2)

	confState = pb.ConfState{
		Voters: []uint64{111, 222, 333, 444},
	}

	m := refs.NewMember(refs.PeerID(444), []string{"127.0.0.4:2022"})
	member := []refs.Member{
		m,
	}

	s.NoError(s.storage.OnConfState(3, confState, member, pb.ConfChangeAddNode))
	s.checkIndex(3, 2)
	addrs, err := s.storage.GetMemberAddrs(refs.PeerID(111))
	s.NoError(err)
	s.EqualValues(strings.Join(addrs, ","), "http://127.0.0.1:2022")

	confState = pb.ConfState{
		Voters: []uint64{111, 222, 333},
	}
	s.NoError(s.storage.OnConfState(4, confState, member, pb.ConfChangeRemoveNode))
	s.checkIndex(4, 2)
	_, err = s.storage.GetMemberAddrs(refs.PeerID(444))
	s.Error(err)

	m3 := refs.NewMember(refs.PeerID(444), []string{"127.0.0.4:2022"})
	member3 := []refs.Member{
		m3,
	}
	confState = pb.ConfState{
		Voters:   []uint64{111, 222, 333},
		Learners: []uint64{444},
	}
	s.NoError(s.storage.OnConfState(5, confState, member3, pb.ConfChangeAddLearnerNode))
	s.checkIndex(5, 2)
	addrs3, err := s.storage.GetMemberAddrs(refs.PeerID(333))
	s.NoError(err)
	s.EqualValues(strings.Join(addrs3, ","), "http://127.0.0.3:2022")

	initState, err := s.storage.GetInitState()
	s.NoError(err)
	s.EqualValues(len(initState.Members), 4)

	_, confState, err = s.storage.InitialState()
	s.NoError(err)
	s.Equal([]uint64{111, 222, 333}, confState.Voters)
	s.Equal([]uint64{444}, confState.Learners)

	var refhash1 refs.Hash
	var val1 []byte = []byte("3132333435363738393061626364656631323335")
	copy(refhash1[:], val1)
	refsMap3 := make(map[string]refs.Hash)
	refsMap3["refs/heads/branch1"] = refhash1
	sData.Refs = refsMap3
	sData.Members = append(sData.Members, refs.NewMember(refs.PeerID(444), []string{"127.0.0.4:2022"}))

	snapshot.Data, err = refs.EncodeSnapshot(sData)
	s.NoError(err)
	snapshot.Metadata.Term = 5
	snapshot.Metadata.Index = 10
	testObjSrcId := refs.PeerID(12345678)
	s.Error(s.storage.OnSnapshot(snapshot, testObjSrcId))

	membersHttp1 := make([]refs.Member, 4)
	membersHttp1[0] = refs.NewMember(refs.PeerID(111), []string{"http://127.0.0.1:2022"})
	membersHttp1[1] = refs.NewMember(refs.PeerID(222), []string{"http://127.0.0.2:2022"})
	membersHttp1[2] = refs.NewMember(refs.PeerID(333), []string{"http://127.0.0.3:2022"})
	membersHttp1[3] = refs.NewMember(refs.PeerID(444), []string{"http://127.0.0.4:2022"})
	sData.Members = membersHttp1
	snapshot.Data, err = refs.EncodeSnapshot(sData)
	s.NoError(err)
	snapshot.Metadata.ConfState = confState
	s.NoError(s.storage.OnSnapshot(snapshot, refs.PeerID(111)))

	curSnap, err := s.storage.Snapshot()
	s.Equal(string(snapshot.Data), string(curSnap.Data))
	s.Equal(snapshot.Metadata, curSnap.Metadata)
	sDecodeData, err = refs.DecodeSnapshot(snapshot.Data)
	s.NoError(err)
	s.EqualValues(sDecodeData.Refs["refs/heads/branch1"], refhash1)

	var refhash2 refs.Hash
	var val2 []byte = []byte("3132333435363738393061626364656631323334")
	copy(refhash2[:], val2)
	sData.Refs["refs/heads/master"] = refhash2
	sort.Slice(sData.Members, func(i, j int) bool {
		return sData.Members[i].ID < sData.Members[j].ID
	})
	snapshot.Data, err = refs.EncodeSnapshot(sData)
	s.NoError(err)
	snapshot.Metadata.Term = 5
	snapshot.Metadata.Index = 10
	s.NoError(s.storage.OnSnapshot(snapshot, refs.PeerID(111)))

	curSnap, err = s.storage.Snapshot()
	s.Equal(string(snapshot.Data), string(curSnap.Data))
	s.Equal(snapshot.Metadata, curSnap.Metadata)
	sDecodeData, err = refs.DecodeSnapshot(snapshot.Data)
	s.NoError(err)
	s.EqualValues(sDecodeData.Refs["refs/heads/master"], refhash2)
}
