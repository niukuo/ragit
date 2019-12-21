package dbtest

import (
	"context"

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
	s.True(raft.IsEmptyHardState(state.HardState))
	s.Len(state.Peers, 0)

	s.checkIndex(0, 0)

	snapshot, err := s.storage.Snapshot()
	s.NoError(err)
	s.True(raft.IsEmptySnap(snapshot))

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

	s.NoError(s.storage.OnApply(context.Background(), 2, 1, opAdd))

	snapshot, err = s.storage.Snapshot()
	s.NoError(err)
	s.False(raft.IsEmptySnap(snapshot))
	s.Equal(uint64(2), snapshot.Metadata.Term)
	s.Equal(uint64(1), snapshot.Metadata.Index)
	s.Equal(
		`3132333435363738393061626364656631323334 refs/heads/branch2
3132333435363738393061626364656631323334 refs/heads/master
`, string(snapshot.Data))
	s.checkIndex(0, 1)

	s.NoError(s.storage.OnApply(context.Background(), 3, 2, opUpdateRemove))

	snapshot, err = s.storage.Snapshot()
	s.NoError(err)
	s.False(raft.IsEmptySnap(snapshot))
	s.Equal(uint64(3), snapshot.Metadata.Term)
	s.Equal(uint64(2), snapshot.Metadata.Index)
	s.Equal(
		`3132333435363738393061626364656631323335 refs/heads/master
`, string(snapshot.Data))

	confState := pb.ConfState{
		Nodes: []uint64{111, 222, 333},
	}

	s.NoError(s.storage.OnConfState(3, 3, confState))
	s.checkIndex(3, 2)

	_, confState, err = s.storage.InitialState()
	s.NoError(err)
	s.Equal([]uint64{111, 222, 333}, confState.Nodes)

	snapshot.Data = []byte(
		`3132333435363738393061626364656631323335 refs/heads/branch1
`)
	snapshot.Metadata.Term = 5
	snapshot.Metadata.Index = 10
	testObjSrcId, err := ragit.ParsePeerID("127.0.0.1:8080")
	s.NoError(err)
	s.Error(s.storage.OnSnapshot(snapshot, testObjSrcId))

	testObjSrcId, err = ragit.ParsePeerID("127.0.0.1:8082")
	s.NoError(err)
	s.NoError(s.storage.OnSnapshot(snapshot, testObjSrcId))

	curSnap, err := s.storage.Snapshot()
	s.Equal(string(snapshot.Data), string(curSnap.Data))
	s.Equal(snapshot.Metadata, curSnap.Metadata)

	snapshot.Data = []byte(
		`3132333435363738393061626364656631323339 refs/heads/branch1
3132333435363738393061626364656631323334 refs/heads/master
`)
	snapshot.Metadata.Term = 5
	snapshot.Metadata.Index = 10
	s.NoError(s.storage.OnSnapshot(snapshot, testObjSrcId))

	curSnap, err = s.storage.Snapshot()
	s.Equal(string(snapshot.Data), string(curSnap.Data))
	s.Equal(snapshot.Metadata, curSnap.Metadata)
}
