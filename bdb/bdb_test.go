package bdb_test

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/niukuo/ragit/bdb"
	"github.com/niukuo/ragit/dbtest"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/bbolt"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type bdbSuite struct {
	suite.Suite
	dir        string
	newLocalID func() refs.PeerID
}

func TestBDB(t *testing.T) {
	suite.Run(t, &bdbSuite{})
}

func (s *bdbSuite) SetupTest() {
	dir, err := ioutil.TempDir("", "bdb_test")
	s.NoError(err)

	s.dir = dir
	s.newLocalID = func() refs.PeerID {
		return refs.PeerID(222)
	}
}

func (s *bdbSuite) TearDownTest() {
	os.RemoveAll(s.dir)
}

func (s *bdbSuite) TestDB() {
	suite.Run(s.T(), dbtest.NewDBSuite(func() dbtest.Storage {
		opts := bdb.NewOptions()
		opts.Logger = logging.GetLogger("")
		opts.NewLocalID = s.newLocalID

		db, err := bdb.Open(path.Join(s.dir, "test.db"), opts)
		s.NoError(err)

		return db
	}))
}

func (s *bdbSuite) TestSM() {
	suite.Run(s.T(), dbtest.NewSMSuite(func() dbtest.Storage {
		opts := bdb.NewOptions()
		opts.Listener = &emptyListener{}
		opts.Logger = logging.GetLogger("")
		opts.NewLocalID = s.newLocalID
		db, err := bdb.Open(path.Join(s.dir, "test.db"), opts)
		s.NoError(err)

		m1 := refs.NewMember(refs.PeerID(111), []string{"http://127.0.0.1:2022"})
		m2 := refs.NewMember(refs.PeerID(222), []string{"http://127.0.0.2:2022"})
		m3 := refs.NewMember(refs.PeerID(333), []string{"http://127.0.0.3:2022"})

		members := []refs.Member{
			m1,
			m2,
			m3,
		}
		err = db.Bootstrap(members)
		s.NoError(err)

		return db
	}))
}

func (s *bdbSuite) TestCreateBucket() {
	db, err := bbolt.Open(path.Join(s.dir, "test.db"), 0644, nil)
	s.NoError(err)
	defer db.Close()

	s.NoError(db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
		s.NoError(err)
		s.NotNil(b)
		cnt := 0
		s.NoError(b.ForEach(func(k, v []byte) error {
			fmt.Printf("%s: %s\n", string(k), string(v))
			cnt++
			return nil
		}))
		s.NoError(b.Put([]byte("now"), []byte(time.Now().String())))
		s.NoError(b.Put([]byte(fmt.Sprintf("data%d", cnt)), []byte(time.Now().String())))
		return nil
	}))
}

func (s *bdbSuite) TestDropAndCreateBucket() {
	db, err := bbolt.Open(path.Join(s.dir, "test.db"), 0644, nil)
	s.NoError(err)
	defer db.Close()

	s.NoError(db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("test"))
		s.NoError(err)
		s.NotNil(b)
		cnt := 0
		s.NoError(b.ForEach(func(k, v []byte) error {
			fmt.Printf("%s: %s\n", string(k), string(v))
			cnt++
			return nil
		}))

		s.NoError(tx.DeleteBucket([]byte("test")))
		b, err = tx.CreateBucketIfNotExists([]byte("test"))
		s.NoError(err)

		s.NoError(b.Put([]byte("now"), []byte(time.Now().String())))
		s.NoError(b.Put([]byte(fmt.Sprintf("data%d", time.Now().UnixNano())), []byte(time.Now().String())))
		return nil
	}))
}

func (s *bdbSuite) TestInitBuckets() {

	opts := bdb.NewOptions()
	opts.Logger = logging.GetLogger("")
	opts.NewLocalID = func() refs.PeerID {
		return refs.PeerID(123456)
	}

	// db no data
	storage, err := bdb.Open(path.Join(s.dir, "test.db"), opts)
	s.NoError(err)
	state, err := storage.GetInitState()
	s.NoError(err)
	s.EqualValues(123456, state.LocalID)
	s.Zero(state.ConfIndex)
	s.Len(state.Members, 0)
	members, err := storage.GetAllMemberURLs()
	s.NoError(err)
	s.Len(members, 0)
	storage.Close()

	// members has been initialized
	storage, err = bdb.Open(path.Join(s.dir, "test.db"), opts)
	s.NoError(err)
	state, err = storage.GetInitState()
	s.NoError(err)
	s.EqualValues(123456, state.LocalID)
	s.Zero(state.ConfIndex)
	s.Len(state.Members, 0)
	storage.Close()

	// meta has term/index
	db, err := bbolt.Open(path.Join(s.dir, "test.db/state.db"), 0644, nil)
	s.NoError(err)

	s.NoError(db.Update(func(tx *bbolt.Tx) error {
		metab := tx.Bucket(bdb.BucketMeta)
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], 1)
		s.NoError(metab.Put([]byte("term"), buf[:]))
		s.NoError(metab.Put([]byte("index"), buf[:]))
		return nil
	}))
	db.Close()

	storage, err = bdb.Open(path.Join(s.dir, "test.db"), opts)
	s.NoError(err)
	state, err = storage.GetInitState()
	s.NoError(err)
	s.EqualValues(0, state.ConfIndex)
	s.Len(state.Members, 0)
	storage.Close()

	// upgrade from IPv4
	db, err = bbolt.Open(path.Join(s.dir, "test.db/state.db"), 0644, nil)
	s.NoError(err)

	confState := pb.ConfState{
		Voters: []uint64{680414508648243091},
	}
	val, err := confState.Marshal()
	s.NoError(err)
	s.NoError(db.Update(func(tx *bbolt.Tx) error {
		metab := tx.Bucket(bdb.BucketMeta)
		s.NoError(metab.Put([]byte("conf_state"), val))

		stateb := tx.Bucket(bdb.BucketState)
		s.NoError(stateb.Delete([]byte("local_id")))

		tx.DeleteBucket(bdb.BucketMembers)

		return nil
	}))
	db.Close()

	storage, err = bdb.Open(path.Join(s.dir, "test.db"), opts)
	s.NoError(err)
	state, err = storage.GetInitState()
	s.NoError(err)
	s.Len(state.ConfState.Voters, 1)
	s.EqualValues(680414508648243091, state.ConfState.Voters[0])
	s.Len(state.Members, 1)
	s.EqualValues(680414508648243091, uint64(state.Members[0].ID))
	s.Equal("http://100.81.113.9:8083", state.Members[0].PeerURLs[0])
	storage.Close()
}
