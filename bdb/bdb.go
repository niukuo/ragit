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

var (
	ErrReferenceNotFound = errors.New("reference not found")
)

type Storage = *storage
type storage struct {
	WALStorage
	listener refs.Listener
	db       *bbolt.DB

	state pb.HardState

	logger logging.Logger
}

type Options struct {
	Listener refs.Listener

	Logger logging.Logger

	*WALOptions
}

func NewOptions() *Options {
	o := &Options{
		WALOptions: NewWALOptions(),
	}
	return o
}

func Open(path string,
	opts *Options,
) (Storage, error) {

	walOpts := opts.WALOptions
	if walOpts.Logger == nil {
		walOpts = &*opts.WALOptions
		walOpts.Logger = opts.Logger
	}

	wal, err := OpenWAL(path+"/wal", opts.WALOptions)
	if err != nil {
		return nil, err
	}

	db, err := bbolt.Open(path+"/state.db", 0644, nil)
	if err != nil {
		return nil, err
	}

	s := &storage{
		WALStorage: wal,
		listener:   opts.Listener,
		db:         db,

		logger: opts.Logger,
	}

	if err := s.db.Update(s.init); err != nil {
		return nil, err
	}

	s.state, _, err = s.InitialState()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *storage) init(tx *bbolt.Tx) error {

	metab, err := tx.CreateBucket(BucketMeta)
	if err != nil {
		if err != bbolt.ErrBucketExists {
			return err
		}
		// already inited
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
		return nil
	}

	stateb, err := tx.CreateBucket(BucketState)
	if err != nil {
		return err
	}

	if err := putUint64(metab, map[string]uint64{
		"term":       0,
		"index":      0,
		"conf_index": 0,
	}); err != nil {
		return err
	}

	if err := putUint64(stateb, map[string]uint64{
		"term": 0,
		"vote": 0,
	}); err != nil {
		return err
	}

	if _, err := tx.CreateBucket(BucketRefs); err != nil {
		return err
	}

	return nil
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

func saveConfState(b *bbolt.Bucket, index uint64, confState pb.ConfState) error {
	if err := putUint64(b, map[string]uint64{
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
) error {

	if err := s.WALStorage.SaveWAL(entries); err != nil {
		return err
	}

	if raft.IsEmptyHardState(state) {
		return nil
	}

	if state.Term != s.state.Term || state.Vote != s.state.Vote {
		if err := s.db.Update(func(tx *bbolt.Tx) error {

			b := tx.Bucket(BucketState)
			if err := putUint64(b, map[string]uint64{
				"term": state.Term,
				"vote": state.Vote,
			}); err != nil {
				return err
			}

			return nil
		}); err != nil {
			return err
		}
	}

	s.state = state

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
			snapshot.Metadata.Index,
			snapshot.Metadata.ConfState); err != nil {
			return err
		}

		if err := putUint64(metab, map[string]uint64{
			"term":  snapshot.Metadata.Term,
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
		}}); err != nil {
			return fmt.Errorf("save snapshot wal log, err: %v", err)
		}

		return nil
	}); err != nil {
		return err
	}

	return nil

}

func (s *storage) OnConfState(index uint64, confState pb.ConfState) error {

	if err := s.db.Update(func(tx *bbolt.Tx) error {

		metab := tx.Bucket(BucketMeta)
		if err := saveConfState(metab, index, confState); err != nil {
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

		refsb := tx.Bucket(BucketRefs)

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
			if err := s.listener.Apply(ctx, oplog, out); err != nil {
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

	s.WALStorage.SetSnapshotIndex(index)

	return nil
}

func getInitState(
	stateb *bbolt.Bucket, hardState *pb.HardState,
	metab *bbolt.Bucket, confState *pb.ConfState,
) error {

	getUint64(stateb, map[string]*uint64{
		"term": &hardState.Term,
		"vote": &hardState.Vote,
	})

	getUint64(metab, map[string]*uint64{
		"index": &hardState.Commit,
	})

	if err := confState.Unmarshal(
		metab.Get([]byte("conf_state"))); err != nil {
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

		metab := tx.Bucket(BucketMeta)

		if err := getInitState(
			stateb, &hardState,
			metab, &confState,
		); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return hardState, confState, err
	}

	return hardState, confState, nil
}

func (s *storage) Bootstrap(peers []refs.PeerID) error {
	if len(peers) == 0 {
		return errors.New("cant bootstrap with empty peers")
	}
	s.logger.Info("bootstraping using ", peers)

	var confState pb.ConfState
	for _, peer := range peers {
		confState.Nodes = append(confState.Nodes, uint64(peer))
	}

	data, err := confState.Marshal()
	if err != nil {
		return err
	}

	if err := s.db.Update(func(tx *bbolt.Tx) error {
		stateb := tx.Bucket(BucketState)
		metab := tx.Bucket(BucketMeta)

		var (
			hardState pb.HardState
			confState pb.ConfState
		)

		if err := getInitState(
			stateb, &hardState,
			metab, &confState,
		); err != nil {
			return err
		}

		if hardState.Term == 1 {
			hardState.Term = 0
		}
		if !raft.IsEmptyHardState(hardState) {
			return fmt.Errorf("already bootstrapped: %+v", hardState)
		}

		if confState.Size() != 0 {
			return errors.New("already has members")
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
			"term": 1,
			"vote": 0,
		}); err != nil {
			return err
		}

		if err := s.SaveWAL([]pb.Entry{{
			Term:  1,
			Index: 1,
			Type:  pb.EntryNormal,
			Data:  []byte(fmt.Sprintf("bootstrap %v", peers)),
		}}); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (s *storage) GetInitState() (*ragit.InitialState, error) {

	var state ragit.InitialState

	if err := s.db.View(func(tx *bbolt.Tx) error {
		stateb := tx.Bucket(BucketState)
		metab := tx.Bucket(BucketMeta)

		var hardState pb.HardState

		if err := getInitState(
			stateb, &hardState,
			metab, &state.ConfState,
		); err != nil {
			return err
		}

		getUint64(metab, map[string]*uint64{
			"conf_index": &state.ConfIndex,
			"index":      &state.AppliedIndex,
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
		refsMap := make(map[string]refs.Hash)
		if err := getAllRefs(refsb, refsMap); err != nil {
			return err
		}
		snapshot.Data = refs.EncodeSnapshot(refsMap)
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

func (s *storage) GetRefs(name string) (refs.Hash, error) {
	var hash refs.Hash
	if err := s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(BucketRefs)
		switch v := b.Get([]byte(name)); len(v) {
		case 0:
			return ErrReferenceNotFound
		case refs.HashLen:
			copy(hash[:], v)
			return nil
		default:
			return fmt.Errorf("invalid hash (len = %d) %x", len(v), v)
		}
	}); err != nil {
		return hash, err
	}
	return hash, nil
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
			var term, vote uint64
			getUint64(stateb, map[string]*uint64{
				"term": &term,
				"vote": &vote,
			})
			fmt.Fprintln(w, "term:", term)
			fmt.Fprintln(w, "vote:", ragit.PeerID(vote))
		}

		metab := tx.Bucket(BucketMeta)
		{
			var term, index, confIndex uint64
			getUint64(metab, map[string]*uint64{
				"term":       &term,
				"index":      &index,
				"conf_index": &confIndex,
			})
			fmt.Fprintln(w, "conf_index:", confIndex)

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
