package dbtest

import (
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type DBSuite struct {
	suite.Suite
	db     Storage
	create func() Storage
}

func NewDBSuite(create func() Storage) *DBSuite {
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
		{Term: 1, Index: 3},
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
	}, false))

	first, err = s.db.FirstIndex()
	s.NoError(err)
	s.Equal(uint64(4), first)

	last, err = s.db.LastIndex()
	s.NoError(err)
	s.Equal(uint64(5), last)

	s.NoError(s.db.Save(pb.HardState{}, []pb.Entry{
		{Term: 1, Index: 6},
		{Term: 1, Index: 7},
		{Term: 1, Index: 8},
	}, false))

	first, err = s.db.FirstIndex()
	s.NoError(err)
	s.Equal(uint64(4), first)

	last, err = s.db.LastIndex()
	s.NoError(err)
	s.Equal(uint64(8), last)

	s.NoError(s.db.Save(pb.HardState{}, []pb.Entry{
		{Term: 1, Index: 7},
	}, false))

	first, err = s.db.FirstIndex()
	s.NoError(err)
	s.Equal(uint64(4), first)

	last, err = s.db.LastIndex()
	s.NoError(err)
	s.Equal(uint64(7), last)

	s.NoError(s.db.Save(pb.HardState{}, []pb.Entry{
		{Term: 1, Index: 11},
	}, false))

	first, err = s.db.FirstIndex()
	s.NoError(err)
	s.Equal(uint64(12), first)

	last, err = s.db.LastIndex()
	s.NoError(err)
	s.Equal(uint64(11), last)

	s.Error(s.db.Save(pb.HardState{}, []pb.Entry{
		{Term: 1, Index: 11},
		{Term: 1, Index: 13},
	}, false))

	first, err = s.db.FirstIndex()
	s.NoError(err)
	s.Equal(uint64(12), first)

	last, err = s.db.LastIndex()
	s.NoError(err)
	s.Equal(uint64(11), last)

	s.NoError(s.db.Save(pb.HardState{}, []pb.Entry{
		{Term: 1, Index: 111},
		{Term: 2, Index: 112},
		{Term: 3, Index: 113},
	}, false))

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

	state, err := s.db.GetOrInitState(nil)
	s.NoError(err)
	s.Equal(uint64(0), state.AppliedIndex)
	s.Equal(uint64(0), state.ConfIndex)
	s.True(raft.IsEmptyHardState(state.HardState))
	s.Len(state.Peers, 0)

	var hs pb.HardState
	hs.Term = 2
	s.NoError(s.db.Save(hs, nil, false))
	hardState, confState, err := s.db.InitialState()
	s.False(raft.IsEmptyHardState(hardState))
	s.Len(confState.Nodes, 0)
}
