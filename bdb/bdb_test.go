package bdb_test

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/niukuo/ragit/bdb"
	"github.com/niukuo/ragit/dbtest"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/bbolt"
)

var flagDBPath = flag.String("bdbpath", "mydb", "db path")
var newLocalID = func() refs.PeerID {
	return 222
}

func TestDB(t *testing.T) {
	s := assert.New(t)
	suite.Run(t, dbtest.NewDBSuite(func() dbtest.Storage {
		path := *flagDBPath
		os.RemoveAll(path)
		opts := bdb.NewOptions()
		opts.Logger = logging.GetLogger("")
		opts.NewLocalID = newLocalID
		db, err := bdb.Open(path, opts)
		s.NoError(err)

		return db
	}))
}

func TestSM(t *testing.T) {
	s := assert.New(t)
	suite.Run(t, dbtest.NewSMSuite(func() dbtest.Storage {
		path := *flagDBPath
		os.RemoveAll(path)
		opts := bdb.NewOptions()
		opts.Listener = &emptyListener{}
		opts.Logger = logging.GetLogger("")
		opts.NewLocalID = newLocalID
		db, err := bdb.Open(path, opts)
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

func TestCreateBucket(t *testing.T) {
	s := assert.New(t)
	db, err := bbolt.Open("test.db", 0644, nil)
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

func TestDropAndCreateBucket(t *testing.T) {
	s := assert.New(t)
	db, err := bbolt.Open("test2.db", 0644, nil)
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
