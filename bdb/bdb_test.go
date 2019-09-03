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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/bbolt"
)

var flagDBPath = flag.String("bdbpath", "mydb", "db path")

func TestDB(t *testing.T) {
	s := assert.New(t)
	suite.Run(t, dbtest.NewDBSuite(func() dbtest.Storage {
		path := *flagDBPath
		os.RemoveAll(path)
		opts := bdb.NewOptions()
		opts.Logger = logging.GetLogger("")
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
		db, err := bdb.Open(path, opts)
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
