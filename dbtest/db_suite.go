package dbtest

import (
	"github.com/golang/protobuf/proto"
	ragit "github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type DBSuite struct {
	suite.Suite
	db     ragit.Storage
	create func() ragit.Storage
}

func NewDBSuite(create func() ragit.Storage) *DBSuite {
	s := &DBSuite{
		create: create,
	}
	return s
}

func (s *DBSuite) SetupTest() {
	s.db = s.create()
}

func (s *DBSuite) TearDownTest() {
	s.db.Close()
	s.db = nil
}

func (s *DBSuite) TestWAL() {
	first, err := s.db.FirstIndex()
	s.NoError(err)
	s.Equal(uint64(1), first)
	last, err := s.db.LastIndex()
	s.NoError(err)
	s.Equal(uint64(0), last)

	s.NoError(s.db.Save(pb.HardState{}, []pb.Entry{
		pb.Entry{Term: 1, Index: 3},
		pb.Entry{Term: 1, Index: 4},
		pb.Entry{Term: 1, Index: 5},
	}, pb.Snapshot{}, false))

	first, err = s.db.FirstIndex()
	s.NoError(err)
	s.Equal(uint64(4), first)

	last, err = s.db.LastIndex()
	s.NoError(err)
	s.Equal(uint64(5), last)

	s.NoError(s.db.Save(pb.HardState{}, []pb.Entry{
		pb.Entry{Term: 1, Index: 6},
		pb.Entry{Term: 1, Index: 7},
		pb.Entry{Term: 1, Index: 8},
	}, pb.Snapshot{}, false))

	first, err = s.db.FirstIndex()
	s.NoError(err)
	s.Equal(uint64(4), first)

	last, err = s.db.LastIndex()
	s.NoError(err)
	s.Equal(uint64(8), last)

	s.NoError(s.db.Save(pb.HardState{}, []pb.Entry{
		pb.Entry{Term: 1, Index: 7},
	}, pb.Snapshot{}, false))

	first, err = s.db.FirstIndex()
	s.NoError(err)
	s.Equal(uint64(4), first)

	last, err = s.db.LastIndex()
	s.NoError(err)
	s.Equal(uint64(7), last)

	s.NoError(s.db.Save(pb.HardState{}, []pb.Entry{
		pb.Entry{Term: 1, Index: 11},
	}, pb.Snapshot{}, false))

	first, err = s.db.FirstIndex()
	s.NoError(err)
	s.Equal(uint64(12), first)

	last, err = s.db.LastIndex()
	s.NoError(err)
	s.Equal(uint64(11), last)

	s.Error(s.db.Save(pb.HardState{}, []pb.Entry{
		pb.Entry{Term: 1, Index: 11},
		pb.Entry{Term: 1, Index: 13},
	}, pb.Snapshot{}, false))

	first, err = s.db.FirstIndex()
	s.NoError(err)
	s.Equal(uint64(12), first)

	last, err = s.db.LastIndex()
	s.NoError(err)
	s.Equal(uint64(11), last)

	s.NoError(s.db.Save(pb.HardState{}, []pb.Entry{
		pb.Entry{Term: 1, Index: 111},
		pb.Entry{Term: 2, Index: 112},
		pb.Entry{Term: 3, Index: 113},
	}, pb.Snapshot{}, false))

	ents, err := s.db.Entries(111, 114, 100)
	s.NoError(err)
	s.Len(ents, 3)

	term, err := s.db.Term(110)
	s.Error(err)

	term, err = s.db.Term(111)
	s.NoError(err)
	s.Equal(uint64(1), term)

	term, err = s.db.Term(112)
	s.NoError(err)
	s.Equal(uint64(2), term)

	hardState, confState, err := s.db.InitialState()
	s.True(raft.IsEmptyHardState(hardState))
	s.Len(confState.Nodes, 0)

	var state pb.HardState
	state.Term = 2
	s.NoError(s.db.Save(state, nil, pb.Snapshot{}, false))
	hardState, confState, err = s.db.InitialState()
	s.False(raft.IsEmptyHardState(hardState))
}

func (s *DBSuite) TestApply() {

	hardState, confState, err := s.db.InitialState()
	s.NoError(err)
	s.True(raft.IsEmptyHardState(hardState))
	s.Len(confState.Nodes, 0)
	s.Len(confState.Learners, 0)

	snapshot, err := s.db.Snapshot()
	s.NoError(err)
	s.True(raft.IsEmptySnap(snapshot))

	opAdd := refs.Oplog{
		Ops: []*refs.Oplog_Op{
			&refs.Oplog_Op{
				Name:   proto.String("refs/heads/master"),
				Target: []byte("1234567890abcdef1234"),
			},
			&refs.Oplog_Op{
				Name:   proto.String("refs/heads/branch2"),
				Target: []byte("1234567890abcdef1234"),
			},
		},
	}

	opUpdateRemove := refs.Oplog{
		Ops: []*refs.Oplog_Op{
			&refs.Oplog_Op{
				Name:      proto.String("refs/heads/master"),
				Target:    []byte("1234567890abcdef1235"),
				OldTarget: []byte("1234567890abcdef1234"),
			},
			&refs.Oplog_Op{
				Name:      proto.String("refs/heads/branch2"),
				OldTarget: []byte("1234567890abcdef1234"),
			},
		},
	}

	s.Error(s.db.Apply(1, 2, opAdd, nil))
	s.NoError(s.db.Apply(2, 1, opAdd, nil))

	snapshot, err = s.db.Snapshot()
	s.NoError(err)
	s.False(raft.IsEmptySnap(snapshot))
	s.Equal(uint64(2), snapshot.Metadata.Term)
	s.Equal(uint64(1), snapshot.Metadata.Index)
	s.Equal(
		`3132333435363738393061626364656631323334 refs/heads/branch2
3132333435363738393061626364656631323334 refs/heads/master
`, string(snapshot.Data))

	s.Error(s.db.Apply(3, 1, opUpdateRemove, nil))
	s.NoError(s.db.Apply(3, 2, opUpdateRemove, nil))

	snapshot, err = s.db.Snapshot()
	s.NoError(err)
	s.False(raft.IsEmptySnap(snapshot))
	s.Equal(uint64(3), snapshot.Metadata.Term)
	s.Equal(uint64(2), snapshot.Metadata.Index)
	s.Equal(
		`3132333435363738393061626364656631323335 refs/heads/master
`, string(snapshot.Data))

	confState = pb.ConfState{
		Nodes: []uint64{111, 222, 333},
	}
	s.Error(s.db.UpdateConfState(3, 2, confState))
	s.NoError(s.db.UpdateConfState(3, 3, confState))

	_, confState, err = s.db.InitialState()
	s.NoError(err)
	s.Equal([]uint64{111, 222, 333}, confState.Nodes)

	snapshot.Data = []byte(
		`3132333435363738393061626364656631323335 refs/heads/branch1
`)
	snapshot.Metadata.Term = 5
	snapshot.Metadata.Index = 10
	s.NoError(s.db.Save(pb.HardState{}, nil, snapshot, false))

	curSnap, err := s.db.Snapshot()
	s.Equal(string(snapshot.Data), string(curSnap.Data))
	s.Equal(snapshot.Metadata, curSnap.Metadata)

	snapshot.Data = []byte(
		`3132333435363738393061626364656631323339 refs/heads/branch1
3132333435363738393061626364656631323334 refs/heads/master
`)
	snapshot.Metadata.Term = 5
	snapshot.Metadata.Index = 10
	s.NoError(s.db.Save(pb.HardState{}, nil, snapshot, false))

	curSnap, err = s.db.Snapshot()
	s.Equal(string(snapshot.Data), string(curSnap.Data))
	s.Equal(snapshot.Metadata, curSnap.Metadata)

}
