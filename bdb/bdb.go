package bdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

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
	listener refs.Listener
	db       *bbolt.DB

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

func getUint64(b *bbolt.Bucket, out map[string]*uint64) {
	for k, v := range out {
		*v = binary.BigEndian.Uint64(b.Get([]byte(k)))
	}
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

func saveConfState(b *bbolt.Bucket, term, index uint64, confState pb.ConfState) error {
	if err := putUint64(b, map[string]uint64{
		"term":       term,
		"conf_index": index,
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
	sync bool) error {

	if len(entries) > 0 {
		if err := s.WALStorage.SaveWAL(entries, sync); err != nil {
			return err
		}
	}

	if !raft.IsEmptyHardState(state) {
		if err := s.db.Update(func(tx *bbolt.Tx) error {

			b := tx.Bucket(BucketState)
			if err := putUint64(b, map[string]uint64{
				"term":   state.Term,
				"vote":   state.Vote,
				"commit": state.Commit,
			}); err != nil {
				return err
			}

			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

func (s *storage) OnStart() error {

	refsMap := make(map[string]refs.Hash)

	if err := s.db.View(func(tx *bbolt.Tx) error {
		refsb := tx.Bucket(BucketRefs)
		if err := getAllRefs(refsb, refsMap); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	if err := s.listener.Check(refsMap); err != nil {
		return err
	}

	return nil
}

func (s *storage) OnSnapshot(
	snapshot pb.Snapshot,
	objSrcNode ragit.PeerID,
) error {

	if objSrcNode == refs.PeerID(0) {
		return errors.New("objSrcNode should not be zero when saving snapshot")
	}

	refsMap, err := refs.DecodeSnapshot(snapshot.Data)
	if err != nil {
		return err
	}

	peers := make([]ragit.PeerID, 0, len(snapshot.Metadata.ConfState.Nodes))
	for _, peer := range snapshot.Metadata.ConfState.Nodes {
		peers = append(peers, ragit.PeerID(peer))
	}

	s.logger.Info("saving snapshot from ", objSrcNode,
		", term: ", snapshot.Metadata.Term,
		", index: ", snapshot.Metadata.Index,
		", peers: ", peers)

	if err := s.db.Update(func(tx *bbolt.Tx) error {

		switch err := tx.DeleteBucket(BucketRefs); err {
		case nil, bbolt.ErrBucketNotFound:
		default:
			return err
		}
		refsb, err := tx.CreateBucket(BucketRefs)
		if err != nil {
			return err
		}

		for name, hash := range refsMap {
			hash := hash
			if err := refsb.Put([]byte(name), hash[:]); err != nil {
				return err
			}
		}

		metab := tx.Bucket(BucketMeta)
		if err := saveConfState(metab,
			snapshot.Metadata.Term,
			snapshot.Metadata.Index,
			snapshot.Metadata.ConfState); err != nil {
			return err
		}

		if err := putUint64(metab, map[string]uint64{
			"index": snapshot.Metadata.Index,
		}); err != nil {
			return err
		}

		if err := s.listener.FetchObjects(refsMap, objSrcNode); err != nil {
			return err
		}

		if err := s.WALStorage.SaveWAL([]pb.Entry{{
			Type:  pb.EntryNormal,
			Index: snapshot.Metadata.Index,
			Term:  snapshot.Metadata.Term,
			Data: append([]byte(fmt.Sprintf("snapshot from %s: ", objSrcNode)),
				snapshot.Data...),
		}}, true); err != nil {
			return fmt.Errorf("save snapshot wal log, err: %v", err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil

}

func (s *storage) OnConfState(term, index uint64, confState pb.ConfState) error {

	if err := s.db.Update(func(tx *bbolt.Tx) error {

		metab := tx.Bucket(BucketMeta)
		if err := saveConfState(metab, term, index, confState); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (s *storage) OnApply(ctx context.Context, term, index uint64, oplog refs.Oplog) error {

	var buf bytes.Buffer
	var out io.Writer = &buf
	if v := ctx.Value(ragit.CtxWriterKey); v != nil {
		out = io.MultiWriter(&buf, v.(io.Writer))
	}

	var errSkip = errors.New("check failed, skip")

	if err := s.db.Update(func(tx *bbolt.Tx) error {
		metab := tx.Bucket(BucketMeta)
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
			if err = s.listener.Apply(ctx, oplog, out); err != nil {
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
		}

		return nil
	}); err != nil && err != errSkip {
		return err
	}

	return nil
}

func (s *storage) InitialState() (pb.HardState, pb.ConfState, error) {
	var (
		hardState pb.HardState
		confState pb.ConfState
	)

	if err := s.db.View(func(tx *bbolt.Tx) error {
		stateb := tx.Bucket(BucketState)
		getUint64(stateb, map[string]*uint64{
			"term":   &hardState.Term,
			"vote":   &hardState.Vote,
			"commit": &hardState.Commit,
		})

		metab := tx.Bucket(BucketMeta)
		if err := confState.Unmarshal(
			metab.Get([]byte("conf_state"))); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return hardState, confState, err
	}

	return hardState, confState, nil
}

func (s *storage) initBucket(
	metab *bbolt.Bucket,
	stateb *bbolt.Bucket,
	peers []refs.PeerID) error {

	if len(peers) > 0 {
		s.logger.Info("bootstraping using ", peers)
		var confState pb.ConfState
		for _, peer := range peers {
			confState.Nodes = append(confState.Nodes, uint64(peer))
		}

		data, err := confState.Marshal()
		if err != nil {
			return err
		}

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
			"term":   1,
			"commit": 1,
			"vote":   0,
		}); err != nil {
			return err
		}

		if err := s.SaveWAL([]pb.Entry{{
			Term:  1,
			Index: 1,
			Type:  pb.EntryNormal,
			Data:  []byte(fmt.Sprintf("bootstrap %v", peers)),
		}}, true); err != nil {
			return err
		}

		return nil
	}

	if err := putUint64(metab, map[string]uint64{
		"term":       0,
		"index":      0,
		"conf_index": 0,
	}); err != nil {
		return err
	}

	if err := putUint64(stateb, map[string]uint64{
		"term":   0,
		"commit": 0,
		"vote":   0,
	}); err != nil {
		return err
	}

	return nil
}

func (s *storage) GetOrInitState(peers []refs.PeerID) (*ragit.InitialState, error) {

	var state ragit.InitialState

	if err := s.db.Update(func(tx *bbolt.Tx) error {
		metab, err := tx.CreateBucket(BucketMeta)
		var stateb *bbolt.Bucket
		switch err {
		case nil:
			stateb, err = tx.CreateBucket(BucketState)
			if err != nil {
				return err
			}
			if err := s.initBucket(metab, stateb, peers); err != nil {
				return err
			}
		case bbolt.ErrBucketExists:
			metab = tx.Bucket(BucketMeta)
			if v := metab.Get([]byte("conf_index")); v == nil {
				idx := binary.BigEndian.Uint64(metab.Get([]byte("index")))
				s.logger.Warning("filling conf_index to ", idx)
				if err := putUint64(metab, map[string]uint64{
					"conf_index": idx,
				}); err != nil {
					return err
				}
			}
			stateb = tx.Bucket(BucketState)
		default:
			return err
		}
		var confState pb.ConfState
		if err := confState.Unmarshal(
			metab.Get([]byte("conf_state"))); err != nil {
			return err
		}
		peers = make([]refs.PeerID, 0, len(confState.Nodes))
		for _, peer := range confState.Nodes {
			peers = append(peers, refs.PeerID(peer))
		}
		state.Peers = peers
		getUint64(metab, map[string]*uint64{
			"conf_index": &state.ConfIndex,
			"index":      &state.AppliedIndex,
		})

		stateb = tx.Bucket(BucketState)
		getUint64(stateb, map[string]*uint64{
			"term":   &state.HardState.Term,
			"vote":   &state.HardState.Vote,
			"commit": &state.HardState.Commit,
		})

		return nil
	}); err != nil {
		return nil, err
	}

	return &state, nil
}

func (s *storage) Snapshot() (pb.Snapshot, error) {
	var snapshot pb.Snapshot

	if err := s.db.View(func(tx *bbolt.Tx) error {
		metab := tx.Bucket(BucketMeta)
		if err := snapshot.Metadata.ConfState.Unmarshal(
			metab.Get([]byte("conf_state"))); err != nil {
			return err
		}
		getUint64(metab, map[string]*uint64{
			"term":  &snapshot.Metadata.Term,
			"index": &snapshot.Metadata.Index,
		})
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

func (s *storage) Describe(w io.Writer) {

	fmt.Fprintln(w, "\nstorage:")

	s.WALStorage.Describe(w)
	if err := s.db.View(func(tx *bbolt.Tx) error {
		stateb := tx.Bucket(BucketState)
		{
			var term, vote, commit uint64
			getUint64(stateb, map[string]*uint64{
				"term":   &term,
				"vote":   &vote,
				"commit": &commit,
			})
			fmt.Fprintln(w, "term:", term)
			fmt.Fprintln(w, "vote:", ragit.PeerID(vote))
			fmt.Fprintln(w, "committed_index:", commit)
		}

		metab := tx.Bucket(BucketMeta)
		{
			var term, index, confIndex uint64
			getUint64(metab, map[string]*uint64{
				"term":       &term,
				"index":      &index,
				"conf_index": &confIndex,
			})
			fmt.Fprintln(w, "disk_conf_index:", confIndex)

			var confState pb.ConfState
			if err := confState.Unmarshal(metab.Get([]byte("conf_state"))); err != nil {
				return err
			}

			peers := confState.Nodes
			fmt.Fprint(w, "peers: [")
			sort.Slice(peers, func(i, j int) bool {
				return peers[i] < peers[j]
			})
			for i, peer := range peers {
				if i != 0 {
					fmt.Fprint(w, " ")
				}
				fmt.Fprint(w, ragit.PeerID(peer))
			}
			fmt.Fprintln(w, "]")

			fmt.Fprintln(w, "snapshot_term:", term)
			fmt.Fprintln(w, "snapshot_index:", index)
		}

		return nil
	}); err != nil {
		fmt.Fprintln(w, "failed: ", err)
	}
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
