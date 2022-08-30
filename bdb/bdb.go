package bdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/niukuo/ragit/logging"
	ragit "github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/bbolt"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

var (
	BucketState   = []byte("state")
	BucketMeta    = []byte("meta")
	BucketRefs    = []byte("refs")
	BucketMembers = []byte("members")
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

	applyWaits *waitApplyRequests
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

type memberInDB struct {
	PeerAddrs []string
}

type GetLocalID func() (refs.PeerID, error)

func Open(path string,
	opts *Options,
	getLocalID GetLocalID,
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

		applyWaits: newWaitApplyRequests(100, opts.Logger),
		logger:     opts.Logger,
	}

	err = s.initBuckets(getLocalID)
	if err != nil {
		return nil, err
	}

	s.state, _, err = s.InitialState()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *storage) initBuckets(getLocalID GetLocalID) error {
	if err := s.db.Update(func(tx *bbolt.Tx) error {
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

			stateb := tx.Bucket(BucketState)
			myID := stateb.Get([]byte("local_id"))
			if myID != nil {
				return nil
			}

			localID, err := getLocalID()
			if err != nil {
				return err
			}
			err = putUint64(stateb, map[string]uint64{
				"local_id": uint64(localID),
			})
			if err != nil {
				return err
			}

			if err := convertToMembers(s, tx); err != nil {
				return err
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

		localID, err := getLocalID()
		if err != nil {
			return err
		}
		if err := putUint64(stateb, map[string]uint64{
			"term":     0,
			"vote":     0,
			"local_id": uint64(localID),
		}); err != nil {
			return err
		}

		if _, err := tx.CreateBucket(BucketRefs); err != nil {
			return err
		}

		if _, err := tx.CreateBucket(BucketMembers); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func convertToMembers(s *storage, tx *bbolt.Tx) error {
	members, err := getMembersByConfState(tx)
	if err != nil {
		return err
	}

	membersb, err := tx.CreateBucket(BucketMembers)
	if err != nil {
		return err
	}

	if err := saveMembers(s, membersb, members, pb.ConfChangeAddNode); err != nil {
		return err
	}

	return nil
}

func getMembersByConfState(tx *bbolt.Tx) ([]*refs.Member, error) {
	metab := tx.Bucket(BucketMeta)
	var confState pb.ConfState
	if err := confState.Unmarshal(metab.Get([]byte("conf_state"))); err != nil {
		return nil, err
	}

	confMembers := append(confState.Voters, confState.Learners...)
	members := make([]*refs.Member, len(confMembers))
	for idx, cMemberID := range confMembers {
		id := refs.PeerID(cMemberID)
		m := refs.NewMember(refs.PeerID(cMemberID), []string{fmt.Sprintf("http://%v:%v", uint32(id>>32), uint16(id))})
		members[idx] = m
	}

	return members, nil
}

func (s *storage) Close() {
	s.db.Close()
	s.db = nil
	s.WALStorage.Close()
	s.WALStorage = nil
}

func getAllMembers(membersb *bbolt.Bucket) ([]*refs.Member, error) {
	members := make([]*refs.Member, 0)
	if err := membersb.ForEach(func(k, v []byte) error {
		mID := refs.PeerID(binary.BigEndian.Uint64(k))
		var mInDB memberInDB
		err := json.Unmarshal(v, &mInDB)
		if err != nil {
			return err
		}
		members = append(members, &refs.Member{
			ID:        mID,
			PeerAddrs: mInDB.PeerAddrs,
		})
		return nil
	}); err != nil {
		return nil, err
	}

	return members, nil
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

func (s *storage) GetMemberAddrs(memberID refs.PeerID) ([]string, error) {
	var addrs []string
	if err := s.db.View(func(tx *bbolt.Tx) error {
		membersb := tx.Bucket(BucketMembers)

		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(memberID))
		val := membersb.Get(key[:])
		if val == nil {
			return fmt.Errorf("member not exist, memberID: %v", uint64(memberID))
		}

		var mInDB memberInDB
		if err := json.Unmarshal(val, &mInDB); err != nil {
			return fmt.Errorf("unmarshal failed, val: %v", string(val))
		}

		addrs = mInDB.PeerAddrs
		return nil
	}); err != nil {
		return nil, err
	}

	return addrs, nil
}

func saveMembers(s *storage, membersb *bbolt.Bucket, members []*refs.Member, confType pb.ConfChangeType) error {
	switch confType {
	case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode:
		for _, m := range members {
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], uint64(m.ID))
			mInDB := memberInDB{PeerAddrs: m.PeerAddrs}
			val, err := json.Marshal(mInDB)
			if err != nil {
				return err
			}
			if err := membersb.Put(key[:], val); err != nil {
				return err
			}

		}
	case pb.ConfChangeRemoveNode:
		for _, m := range members {
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], uint64(m.ID))
			if err := membersb.Delete(key[:]); err != nil {
				return err
			}

		}
	default:
		return fmt.Errorf("unsupported conf type: %v", confType)
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

	if err := s.db.View(func(tx *bbolt.Tx) error {
		metab := tx.Bucket(BucketMeta)
		var index uint64
		getUint64(metab, map[string]*uint64{
			"index": &index,
		})
		s.applyWaits.applyDataIndex(index)

		return nil
	}); err != nil {
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

	snapshotData, err := refs.DecodeSnapshot(snapshot.Data)
	if err != nil {
		return err
	}

	if err := checkMembers(snapshotData.Members, snapshot.Metadata.ConfState); err != nil {
		return err
	}

	peers := make([]ragit.PeerID, 0, len(snapshot.Metadata.ConfState.Voters))
	for _, peer := range snapshot.Metadata.ConfState.Voters {
		peers = append(peers, ragit.PeerID(peer))
	}

	learners := make([]ragit.PeerID, 0, len(snapshot.Metadata.ConfState.Learners))
	for _, learner := range snapshot.Metadata.ConfState.Learners {
		learners = append(learners, ragit.PeerID(learner))
	}

	s.logger.Info("saving snapshot from ", objSrcNode,
		", term: ", snapshot.Metadata.Term,
		", index: ", snapshot.Metadata.Index,
		", peers: ", peers,
		", learners: ", learners,
		", members:", snapshotData.Members,
		",")

	snapshotReceive.WithLabelValues(objSrcNode.String()).Inc()

	s.applyWaits.applyConfIndex(0)
	s.applyWaits.applyDataIndex(0)

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

		for name, hash := range snapshotData.Refs {
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

		members := make([]*refs.Member, len(snapshotData.Members))
		var ObjSrcAddrs []string
		for idx, m := range snapshotData.Members {
			members[idx] = m
			if m.ID == objSrcNode {
				ObjSrcAddrs = m.PeerAddrs
			}
		}
		membersb := tx.Bucket(BucketMembers)
		if err := saveMembers(s, membersb, members, pb.ConfChangeAddNode); err != nil {
			return err
		}

		start := time.Now()

		if err := s.listener.FetchObjects(snapshotData.Refs, ObjSrcAddrs); err != nil {
			return err
		}

		snapshotFetchObjectsSeconds.WithLabelValues(
			objSrcNode.String()).Observe(time.Since(start).Seconds())

		if err := s.WALStorage.SaveWAL([]pb.Entry{{
			Type:  pb.EntryNormal,
			Index: snapshot.Metadata.Index,
			Term:  snapshot.Metadata.Term,
			Data: append([]byte(fmt.Sprintf("snapshot from %s: ", objSrcNode)),
				snapshot.Data...),
		}}); err != nil {
			return fmt.Errorf("save snapshot wal log, err: %w", err)
		}

		return nil
	}); err != nil {
		return err
	}

	s.applyWaits.applyConfIndex(snapshot.Metadata.Index)
	s.applyWaits.applyDataIndex(snapshot.Metadata.Index)

	return nil

}

func (s *storage) OnConfState(index uint64, confState pb.ConfState, changeMembers []*refs.Member, opType pb.ConfChangeType) error {

	if err := s.db.Update(func(tx *bbolt.Tx) error {

		metab := tx.Bucket(BucketMeta)
		if err := saveConfState(metab, index, confState); err != nil {
			return err
		}

		membersb := tx.Bucket(BucketMembers)
		err := saveMembers(s, membersb, changeMembers, opType)
		if err != nil {
			return err
		}

		existMembers, err := getAllMembers(membersb)
		if err != nil {
			return err
		}

		if err := checkMembers(existMembers, confState); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	s.applyWaits.applyConfIndex(index)
	return nil
}

func checkMembers(existMembers []*refs.Member, confState pb.ConfState) error {
	if len(confState.Learners)+len(confState.Voters) != len(existMembers) {
		return fmt.Errorf("len not equal, confState: %+v, existMembers: %+v", confState, existMembers)
	}

	confMembers := append(confState.Voters, confState.Learners...)
	for _, voter := range confMembers {
		voterID := refs.PeerID(voter)
		ok := false
		for _, m := range existMembers {
			if m.ID == voterID {
				ok = true
			}
		}
		if !ok {
			return fmt.Errorf("member Inconsistent,confState: %+v, existMembers: %+v", confState, existMembers)
		}
	}

	return nil
}

func (s *storage) GetConfState() (*pb.ConfState, error) {
	var confState pb.ConfState

	if err := s.db.View(func(tx *bbolt.Tx) error {
		metab := tx.Bucket(BucketMeta)

		if err := confState.Unmarshal(metab.Get([]byte("conf_state"))); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return &confState, nil
}

func (s *storage) OnApply(term, index uint64, oplog refs.Oplog, handle refs.ReqHandle) error {
	start := time.Now()

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
				err := fmt.Errorf("old target check failed for %s, expected: %x, actual: %x",
					name, oldTarget, currentTarget)
				s.logger.Warning(err)
				return err
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

		if err := s.listener.Apply(oplog, handle); err != nil {
			return err
		}

		return nil
	}); err != nil && err != errSkip {
		return err
	}

	s.applyWaits.applyDataIndex(index)

	s.WALStorage.SetSnapshotIndex(index)

	applyOplogSeconds.Observe(time.Since(start).Seconds())

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

func (s *storage) Bootstrap(members []*refs.Member) error {
	if len(members) == 0 {
		return errors.New("cant bootstrap with empty peers")
	}
	s.logger.Info("bootstraping using ", members)

	var confState pb.ConfState
	for _, member := range members {
		confState.Voters = append(confState.Voters, uint64(member.ID))
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

		if len(confState.Voters)+len(confState.Learners) > 0 {
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

		membersb := tx.Bucket(BucketMembers)
		if err := saveMembers(s, membersb, members, pb.ConfChangeAddNode); err != nil {
			return err
		}

		if err := s.SaveWAL([]pb.Entry{{
			Term:  1,
			Index: 1,
			Type:  pb.EntryNormal,
			Data:  []byte(fmt.Sprintf("bootstrap %+v", members)),
		}}); err != nil {
			return err
		}

		s.applyWaits.applyDataIndex(1)
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
		membersb := tx.Bucket(BucketMembers)

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

		getUint64(stateb, map[string]*uint64{
			"local_id": &state.LocalID,
		})

		var err error
		state.Members, err = getAllMembers(membersb)
		if err != nil {
			return err
		}

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

		membersb := tx.Bucket(BucketMembers)
		members, err := getAllMembers(membersb)
		if err != nil {
			return err
		}

		snapshotData := &refs.SnapshotData{
			Refs:    refsMap,
			Members: members,
		}

		snapshot.Data, err = refs.EncodeSnapshot(snapshotData)
		if err != nil {
			return err
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

			describePeers := func(peers []uint64) {
				fmt.Fprint(w, "[")
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
			}

			fmt.Fprint(w, "nodes: ")
			describePeers(confState.Voters)
			fmt.Fprint(w, "learners: ")
			describePeers(confState.Learners)

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

func (s *storage) WaitForApplyIndex(ctx context.Context, appliedIndex uint64) error {
	req, err := s.applyWaits.addRequest(appliedIndex)
	if err != nil {
		return err
	}
	if req == nil {
		return nil
	}

	defer s.applyWaits.delRequest(req.reqID)

	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	select {
	case <-req.resCh:
		return nil
	case <-ctxWithTimeout.Done():
		return ctx.Err()
	}
}

func (s *storage) OnConfIndexChange(confIndex uint64) {
	s.applyWaits.applyConfIndex(confIndex)
}
