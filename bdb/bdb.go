package bdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/niukuo/ragit/logging"
	ragit "github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/bbolt"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
	"gopkg.in/src-d/go-git.v4/plumbing/protocol/packp"
)

var (
	BucketState = []byte("state")
	BucketMeta  = []byte("meta")
	BucketRefs  = []byte("refs")
)

type Storage = *storage
type storage struct {
	WALStorage
	listener     refs.Listener
	db           *bbolt.DB
	indexMatched bool
	appliedIndex uint64

	logger logging.Logger
}

func Open(path string,
	listener refs.Listener,
	logger logging.Logger) (Storage, error) {
	wal, err := OpenWAL(path + "/wal")
	if err != nil {
		return nil, err
	}

	db, err := bbolt.Open(path+"/state.db", 0644, nil)
	if err != nil {
		return nil, err
	}

	s := &storage{
		WALStorage: wal,
		listener:   listener,
		db:         db,

		logger: logger,
	}

	return s, nil
}

func (s *storage) Close() {
	s.db.Close()
	s.db = nil
	s.WALStorage.Close()
	s.WALStorage = nil
}

func putUint64(b *bbolt.Bucket, data map[string]uint64) error {
	for k, v := range data {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], v)
		if err := b.Put([]byte(k), buf[:]); err != nil {
			return err
		}
	}
	return nil
}

func saveMeta(b *bbolt.Bucket, term, index uint64, confState pb.ConfState) error {
	if err := putUint64(b, map[string]uint64{
		"term":  term,
		"index": index,
	}); err != nil {
		return err
	}

	data, err := confState.Marshal()
	if err != nil {
		return err
	}

	if err := b.Put([]byte("conf_state"), data); err != nil {
		return err
	}

	return nil
}

func (s *storage) Save(
	state pb.HardState,
	entries []pb.Entry,
	snapshot pb.Snapshot,
	objSrcNode ragit.PeerID,
	sync bool) error {

	if len(entries) > 0 {
		if err := s.WALStorage.SaveWAL(entries, sync); err != nil {
			return err
		}
	}

	if err := s.db.Update(func(tx *bbolt.Tx) error {

		if !raft.IsEmptyHardState(state) {
			b, err := tx.CreateBucketIfNotExists(BucketState)
			if err != nil {
				return err
			}
			if err := putUint64(b, map[string]uint64{
				"term":   state.Term,
				"vote":   state.Vote,
				"commit": state.Commit,
			}); err != nil {
				return err
			}
		}

		if !raft.IsEmptySnap(snapshot) {
			if objSrcNode == refs.PeerID(0) {
				return errors.New("objSrcNode should not be zero when snapshot is not empty")
			}

			switch err := tx.DeleteBucket(BucketRefs); err {
			case nil, bbolt.ErrBucketNotFound:
			default:
				return err
			}
			refsb, err := tx.CreateBucketIfNotExists(BucketRefs)
			if err != nil {
				return err
			}

			refsMap, err := refs.DecodeSnapshot(snapshot.Data)
			if err != nil {
				return err
			}

			if len(refsMap) > 0 {
				err := s.listener.FetchObjects(refsMap, objSrcNode)
				if err != nil {
					return err
				}
				lastIndex, err := s.WALStorage.LastIndex()
				if err != nil {
					return fmt.Errorf("fail to get last index, err: %v", err)
				}
				if lastIndex < snapshot.Metadata.Index {
					oplog := refs.Oplog{
						ObjPack: snapshot.Data,
						Params:  []string{"snapshot"},
					}
					oplogContent, err := proto.Marshal(&oplog)
					if err != nil {
						return fmt.Errorf("fail to marshal oplogConent, err: %v", err)
					}
					err = s.WALStorage.SaveWAL([]pb.Entry{{
						Type:  pb.EntryNormal,
						Index: snapshot.Metadata.Index,
						Term:  snapshot.Metadata.Term,
						Data:  oplogContent,
					}}, true)
					if err != nil {
						return fmt.Errorf("save snapshot wal log, err: %v", err)
					}
				}
			}

			for name, hash := range refsMap {
				hash := hash
				if err := refsb.Put([]byte(name), hash[:]); err != nil {
					return err
				}
			}

			metab, err := tx.CreateBucketIfNotExists(BucketMeta)
			if err != nil {
				return err
			}

			if err := saveMeta(metab,
				snapshot.Metadata.Term,
				snapshot.Metadata.Index,
				snapshot.Metadata.ConfState); err != nil {
				return err
			}
		}

		return nil

	}); err != nil {
		return err
	}
	if !raft.IsEmptySnap(snapshot) {
		s.indexMatched = true
		s.appliedIndex = snapshot.Metadata.Index
	}
	return nil
}

func (s *storage) checkIndex(index uint64) error {
	if index > s.appliedIndex+1 || s.indexMatched && index <= s.appliedIndex {
		return fmt.Errorf("index gap, expected: %v, actual: %v",
			s.appliedIndex+1, index)
	}
	return nil
}

func (s *storage) UpdateConfState(term, index uint64, confState pb.ConfState) error {

	if err := s.checkIndex(index); err != nil {
		return err
	}
	if !s.indexMatched && index <= s.appliedIndex {
		return nil
	}

	if err := s.db.Update(func(tx *bbolt.Tx) error {
		metab, err := tx.CreateBucketIfNotExists(BucketMeta)
		if err != nil {
			return err
		}

		if err := saveMeta(metab, term, index, confState); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	s.indexMatched = true
	s.appliedIndex = index

	return nil
}

func (s *storage) Apply(term, index uint64, oplog refs.Oplog, srcId ragit.PeerID, retCh <-chan ragit.ApplyResult) error {
	if err := s.checkIndex(index); err != nil {
		return err
	}
	if !s.indexMatched && index <= s.appliedIndex {
		return nil
	}

	var buf bytes.Buffer
	var out io.Writer = &buf
	if retCh != nil {
		v, ok := <-retCh
		if ok {
			defer v.Close()
			out = io.MultiWriter(&buf, v)
		}
	}

	var errSkip = errors.New("check failed, skip")

	if err := s.db.Update(func(tx *bbolt.Tx) error {
		metab, err := tx.CreateBucketIfNotExists(BucketMeta)
		if err != nil {
			return err
		}
		if err := putUint64(metab, map[string]uint64{
			"term":  term,
			"index": index,
		}); err != nil {
			return err
		}

		if len(oplog.GetOps()) == 0 {
			return nil
		}

		refsb, err := tx.CreateBucketIfNotExists(BucketRefs)
		if err != nil {
			return err
		}

		for _, op := range oplog.GetOps() {
			name := []byte(op.GetName())
			target := op.GetTarget()
			oldTarget := op.GetOldTarget()
			currentTarget := refsb.Get(name)
			if !bytes.Equal(oldTarget, currentTarget) {
				s.logger.Warningf("old target check failed for %s, expected: %x, actual: %x",
					name, oldTarget, currentTarget)
				if out != nil {
					refs.ReportError(out, fmt.Errorf("fetch first"))
				}
				return nil
			}
			switch len(target) {
			case 0:
			case refs.HashLen:
			default:
				return fmt.Errorf("invalid target %x for %s", target, op.GetName())
			}
		}

		for _, op := range oplog.GetOps() {
			name := []byte(op.GetName())
			target := op.GetTarget()
			switch len(target) {
			case 0:
				if err := refsb.Delete([]byte(name)); err != nil {
					return err
				}
			case refs.HashLen:
				if err := refsb.Put([]byte(name), target); err != nil {
					return err
				}
			}
		}

		if len(oplog.Ops) > 0 {
			if err = s.listener.Apply(oplog, out); err != nil {
				refs.ReportError(out, err)
				return err
			}

			var status packp.ReportStatus
			if err := status.Decode(bytes.NewReader(buf.Bytes())); err != nil {
				s.logger.Warning("decode report status err: ", err,
					", out: \n", buf.String())
				return err
			}

			if err := status.Error(); err != nil {
				s.logger.Warning("refs not updated, err: ", err)
				return err
			}
		} else if len(oplog.GetParams()) > 0 && oplog.GetParams()[0] == "snapshot" {
			refsMap, err := refs.DecodeSnapshot(oplog.ObjPack)
			if err != nil {
				return err
			}
			if len(refsMap) > 0 {
				if srcId == ragit.PeerID(0) {
					err = s.listener.Reset(refsMap)
				} else {
					err = s.listener.FetchObjects(refsMap, srcId)
				}
				if err != nil {
					return err
				}
			}
		}

		return nil
	}); err != nil && err != errSkip {
		return err
	}

	s.indexMatched = true
	s.appliedIndex = index

	return nil
}

func (s *storage) InitialState() (pb.HardState, pb.ConfState, error) {
	var (
		hardState pb.HardState
		confState pb.ConfState
		refsMap   = make(map[string]refs.Hash)
	)

	if err := s.db.View(func(tx *bbolt.Tx) error {
		stateb := tx.Bucket(BucketState)
		if stateb != nil {
			hardState.Term = binary.BigEndian.Uint64(stateb.Get([]byte("term")))
			hardState.Vote = binary.BigEndian.Uint64(stateb.Get([]byte("vote")))
			hardState.Commit = binary.BigEndian.Uint64(stateb.Get([]byte("commit")))
		}

		refsb := tx.Bucket(BucketRefs)
		if err := getAllRefs(refsb, refsMap); err != nil {
			return err
		}

		metab := tx.Bucket(BucketMeta)
		if metab != nil {
			if err := confState.Unmarshal(metab.Get([]byte("conf_state"))); err != nil {
				return err
			}
			s.appliedIndex = binary.BigEndian.Uint64(metab.Get([]byte("index")))
		}

		return nil
	}); err != nil {
		return hardState, confState, err
	}

	if err := s.listener.Reset(refsMap); err != nil {
		return hardState, confState, err
	}

	return hardState, confState, nil
}

func (s *storage) Snapshot() (pb.Snapshot, error) {
	var snapshot pb.Snapshot

	if err := s.db.View(func(tx *bbolt.Tx) error {
		metab := tx.Bucket(BucketMeta)
		if metab != nil {
			if err := snapshot.Metadata.ConfState.Unmarshal(metab.Get([]byte("conf_state"))); err != nil {
				return err
			}
			snapshot.Metadata.Term = binary.BigEndian.Uint64(metab.Get([]byte("term")))
			snapshot.Metadata.Index = binary.BigEndian.Uint64(metab.Get([]byte("index")))
		}
		refsb := tx.Bucket(BucketRefs)
		if refsb != nil {
			refsMap := make(map[string]refs.Hash)
			if err := getAllRefs(refsb, refsMap); err != nil {
				return err
			}
			snapshot.Data = refs.EncodeSnapshot(refsMap)
		}
		return nil
	}); err != nil {
		return snapshot, err
	}

	return snapshot, nil
}

func (s *storage) GetAllRefs() (map[string]refs.Hash, error) {
	ret := make(map[string]refs.Hash)
	if err := s.db.View(func(tx *bbolt.Tx) error {
		return getAllRefs(tx.Bucket(BucketRefs), ret)
	}); err != nil {
		return nil, err
	}
	return ret, nil
}

func (s *storage) OnLeaderStart(term uint64) {
	s.listener.OnLeaderStart(term)
}

func (s *storage) OnLeaderStop() {
	s.listener.OnLeaderStop()
}

func getAllRefs(refsb *bbolt.Bucket, refsMap map[string]refs.Hash) error {
	if refsb == nil {
		return nil
	}

	if err := refsb.ForEach(func(k, v []byte) error {

		var hash refs.Hash
		if len(v) != refs.HashLen {
			return fmt.Errorf("invalid target(len=%d): %s", len(v), string(v))
		}
		copy(hash[:], v)
		refsMap[string(k)] = hash

		return nil
	}); err != nil {
		return err
	}

	return nil
}
