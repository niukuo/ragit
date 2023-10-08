package bdb

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/niukuo/ragit/logging"
	ragit "github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/bbolt"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type Restorer interface {
	Restore(cfg RestoreConfig) error
	Status(dbPath string) (*Status, error)
}

type Status = ragit.InitialState

var _ Restorer = (*restorer)(nil)

type restorer struct {
	logger logging.Logger
}

type Option func(*restorer)

func WithLogger(logger logging.Logger) func(*restorer) {
	return func(r *restorer) {
		r.logger = logger
	}
}

func NewRestorer(opts ...Option) Restorer {
	s := &restorer{
		logger: logging.GetLogger("ragit.bdb.restore"),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

type RestoreConfig struct {
	DataDir       string
	OutputDataDir string

	LocalID refs.PeerID
	Members []refs.Member
}

func (c RestoreConfig) Validate() error {

	if _, err := os.Stat(c.DataDir); err != nil {
		return err
	}

	if _, err := os.Stat(c.OutputDataDir); err == nil {
		return errors.New("output data dir already exists")
	} else if !os.IsNotExist(err) {
		return err
	}

	if c.LocalID == refs.PeerID(0) {
		return ErrLocalIDIsZero
	}

	if len(c.Members) == 0 {
		return errors.New("cant restore with empty members")
	}

	return nil
}

// Restore restores a new data directory by setting up a Storage that has a first index > 1 and
// reset the desired conf state as its InitialState.
// Notice this function is supposed to be inovoked when the majority of the replication group are
// dead and you'd like to revive the service in the consideration of availability. Be careful.
func (s *restorer) Restore(cfg RestoreConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}

	dataDir := cfg.DataDir
	outputDir := cfg.OutputDataDir

	s.logger.Info("restoring db and wal",
		", data dir: ", dataDir,
		", output data dir: ", outputDir,
		", local id: ", cfg.LocalID,
		", members: ", cfg.Members,
	)

	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}

	srcDbPath := filepath.Join(dataDir, "state.db")
	dbPath := filepath.Join(outputDir, "state.db")
	if err := CopyFile(srcDbPath, dbPath); err != nil {
		return err
	}

	db, err := bbolt.Open(dbPath, 0644, nil)
	if err != nil {
		return err
	}
	defer db.Close()

	members := cfg.Members

	var confState pb.ConfState
	for _, member := range members {
		confState.Voters = append(confState.Voters, uint64(member.ID))
	}
	data, err := confState.Marshal()
	if err != nil {
		return err
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
		stateb := tx.Bucket(BucketState)
		metab := tx.Bucket(BucketMeta)

		if err := putUint64(metab, map[string]uint64{
			"term":       1,
			"index":      1,
			"conf_index": 1,
		}); err != nil {
			return err
		}

		if err := metab.Put([]byte("conf_state"), data); err != nil {
			return err
		}

		if err := putUint64(stateb, map[string]uint64{
			"term":     1,
			"vote":     0,
			"local_id": uint64(cfg.LocalID),
		}); err != nil {
			return err
		}

		membersb := tx.Bucket(BucketMembers)
		if err := membersb.ForEach(func(k, v []byte) error {
			return membersb.Delete(k)
		}); err != nil {
			return err
		}
		if err := onMemberChange(membersb, cfg.Members, pb.ConfChangeAddNode); err != nil {
			return err
		}

		walDir := filepath.Join(outputDir, "wal")

		opt := NewWALOptions()
		opt.Logger = s.logger
		wal, err := OpenWAL(walDir, opt)
		if err != nil {
			return err
		}

		if err := wal.SaveWAL([]pb.Entry{{
			Term:  1,
			Index: 1,
			Type:  pb.EntryNormal,
			Data:  []byte(fmt.Sprintf("restore with %+v", members)),
		}}); err != nil {
			return err
		}
		return nil

	}); err != nil {
		return err
	}

	s.logger.Info("restored db and wal",
		", data dir: ", dataDir,
		", output data dir: ", outputDir,
	)

	return nil
}

func (s *restorer) Status(dbPath string) (*ragit.InitialState, error) {
	db, err := bbolt.Open(dbPath, 0644, nil)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	return getInitStateWithDB(db)
}

func CopyFile(src, dst string) error {
	srcf, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcf.Close()

	srcInfo, err := srcf.Stat()
	if err != nil {
		return err
	}

	dstf, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstf.Close()

	if _, err := io.Copy(dstf, srcf); err != nil {
		return err
	}

	if err := dstf.Chmod(srcInfo.Mode()); err != nil {
		return err
	}

	return nil
}
