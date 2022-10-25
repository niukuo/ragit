package bdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"sync/atomic"
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
	ErrMemberIDNotFound  = errors.New("member id not found")
	ErrLocalIDIsZero     = errors.New("local_id cannot be 0")
)

type Storage = *storage
type storage struct {
	WALStorage
	listener refs.Listener
	db       *bbolt.DB

	state pb.HardState

	leaderTerm uint64

	logger logging.Logger

	applyWaits *waitApplyRequests
}

type Options struct {
	Listener refs.Listener

	Logger logging.Logger

	*WALOptions

	NewLocalID func() (refs.PeerID, error)
}

func NewOptions() *Options {
	o := &Options{
		WALOptions: NewWALOptions(),
	}
	return o
}

type memberAttributes struct {
	PeerURLs []string `json:"peer_urls"`
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

		applyWaits: newWaitApplyRequests(100, opts.Logger),
		logger:     opts.Logger,
	}

	err = s.initBuckets(opts.NewLocalID)
	if err != nil {
		return nil, err
	}

	s.state, _, err = s.InitialState()
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (s *storage) initBuckets(newLocalID func() (refs.PeerID, error)) error {
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
			if id := stateb.Get([]byte("local_id")); id != nil {
				return nil
			}

			localID, err := newLocalID()
			if err != nil {
				return err
			}
			if localID == refs.PeerID(0) {
				return ErrLocalIDIsZero
			}

			s.logger.Warning("filling local_id to ", localID)

			if err := putUint64(stateb, map[string]uint64{
				"local_id": uint64(localID),
			}); err != nil {
				return err
			}

			if err := s.convertConfStateToMembers(tx); err != nil {
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

		localID, err := newLocalID()
		if err != nil {
			return err
		}
		if localID == refs.PeerID(0) {
			return ErrLocalIDIsZero
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

func (s *storage) convertConfStateToMembers(tx *bbolt.Tx) error {

	metab := tx.Bucket(BucketMeta)

	var confState pb.ConfState
	if err := confState.Unmarshal(metab.Get([]byte("conf_state"))); err != nil {
		return err
	}

	confIDs := append(confState.Voters, confState.Learners...)

	members := make([]refs.Member, 0, len(confIDs))
	for _, id := range confIDs {
		members = append(members, refs.NewMember(
			refs.PeerID(id), []string{
				ipv4ToHttpAddr(id),
			},
		))
	}

	s.logger.Warningf("convert ConfState: %v to initial members: %v",
		confState, members)

	membersb, err := tx.CreateBucket(BucketMembers)
	if err != nil {
		return err
	}

	if err := onMemberChange(membersb, members, pb.ConfChangeAddNode); err != nil {
		return err
	}

	return nil
}

// Compatible with older version id
// +--------+------------+---------+
// | 32 bit |   16 bit   |  16 bit |
// +--------+------------+---------+
// | IPv4   |   0(index) |   port  |
// +--------+------------+---------+
func ipv4ToHttpAddr(id uint64) string {

	ip := uint32(id >> 32)

	ipv4 := net.IPv4(
		byte(ip),
		byte(ip>>8),
		byte(ip>>16),
		byte(ip>>24),
	)

	port := uint16(id)

	return fmt.Sprintf("http://%s:%v", ipv4, port)
}

func (s *storage) Close() {
	s.db.Close()
	s.db = nil
	s.WALStorage.Close()
	s.WALStorage = nil
}

func getAllMembers(membersb *bbolt.Bucket) ([]refs.Member, error) {
	members := make([]refs.Member, 0)

	if err := membersb.ForEach(func(k, v []byte) error {

		id := refs.PeerID(binary.BigEndian.Uint64(k))

		var attr memberAttributes
		if err := json.Unmarshal(v, &attr); err != nil {
			return err
		}

		members = append(members, refs.NewMember(id, attr.PeerURLs))

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

func (s *storage) GetURLsByMemberID(id refs.PeerID) ([]string, error) {

	var peerURLs []string

	if err := s.db.View(func(tx *bbolt.Tx) error {
		membersb := tx.Bucket(BucketMembers)

		var key [8]byte
		binary.BigEndian.PutUint64(key[:], uint64(id))

		v := membersb.Get(key[:])
		if v == nil {
			return ErrMemberIDNotFound
		}

		var attr memberAttributes
		if err := json.Unmarshal(v, &attr); err != nil {
			return err
		}

		peerURLs = attr.PeerURLs

		return nil

	}); err != nil {
		return nil, err
	}

	return peerURLs, nil
}

func (s *storage) GetAllMemberURLs() (map[refs.PeerID][]string, error) {

	memberURLs := make(map[refs.PeerID][]string, 0)

	if err := s.db.View(func(tx *bbolt.Tx) error {
		membersb := tx.Bucket(BucketMembers)

		members, err := getAllMembers(membersb)
		if err != nil {
			return err
		}

		for _, m := range members {
			memberURLs[m.ID] = m.PeerURLs
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return memberURLs, nil
}

func onMemberChange(membersb *bbolt.Bucket, members []refs.Member, confType pb.ConfChangeType) error {
	switch confType {
	case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode, pb.ConfChangeUpdateNode:
		for _, m := range members {
			var key [8]byte
			binary.BigEndian.PutUint64(key[:], uint64(m.ID))

			attr := memberAttributes{
				PeerURLs: m.PeerURLs,
			}

			val, err := json.Marshal(attr)
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

	if err := validateMembers(snapshotData.Members, snapshot.Metadata.ConfState); err != nil {
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
		", members: ", snapshotData.Members,
	)

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

		var peerURLs []string
		for _, m := range snapshotData.Members {
			if m.ID == objSrcNode {
				peerURLs = m.PeerURLs
				break
			}
		}

		membersb := tx.Bucket(BucketMembers)
		if err := onMemberChange(membersb, snapshotData.Members, pb.ConfChangeAddNode); err != nil {
			return err
		}

		start := time.Now()

		if err := s.listener.FetchObjects(snapshotData.Refs, peerURLs); err != nil {
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

func (s *storage) OnConfState(index uint64, confState pb.ConfState, changeMembers []refs.Member, opType pb.ConfChangeType) error {

	if err := s.db.Update(func(tx *bbolt.Tx) error {

		metab := tx.Bucket(BucketMeta)
		if err := saveConfState(metab, index, confState); err != nil {
			return err
		}

		membersb := tx.Bucket(BucketMembers)
		if err := onMemberChange(membersb, changeMembers, opType); err != nil {
			return err
		}

		existMembers, err := getAllMembers(membersb)
		if err != nil {
			return err
		}

		if err := validateMembers(existMembers, confState); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	s.applyWaits.applyConfIndex(index)
	return nil
}

func validateMembers(members []refs.Member, confState pb.ConfState) error {
	confPeers := append(confState.Voters, confState.Learners...)
	if len(confPeers) != len(members) {
		return fmt.Errorf("len not equal, confState: %v, members: %v",
			confState, members)
	}

	memberMap := make(map[refs.PeerID]struct{}, len(members))
	for _, m := range members {
		memberMap[m.ID] = struct{}{}
	}

	for _, peer := range confPeers {
		peerID := refs.PeerID(peer)
		if _, ok := memberMap[peerID]; !ok {
			return fmt.Errorf("id: %s not found in members: %v, confState: %v",
				peerID, members, confState)
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

func (s *storage) Bootstrap(members []refs.Member) error {
	if len(members) == 0 {
		return errors.New("cant bootstrap with empty members")
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
		if err := onMemberChange(membersb, members, pb.ConfChangeAddNode); err != nil {
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
	if t := atomic.SwapUint64(&s.leaderTerm, term); t != 0 {
		panic(fmt.Errorf("OnLeaderStart called with leaderTerm == %d", t))
	}
	s.logger.Info("on_leader_start, term: ", term)
	s.listener.OnLeaderStart(term)
}

func (s *storage) OnLeaderStop() {
	if curLeaderTerm := atomic.SwapUint64(&s.leaderTerm, 0); curLeaderTerm == 0 {
		panic(errors.New("OnLeaderStop called with leaderTerm == 0"))
	} else {
		s.logger.Warning("on_leader_stop, term: ", curLeaderTerm)
	}
	s.listener.OnLeaderStop()
}

func (s *storage) GetLeaderTerm() uint64 {
	return atomic.LoadUint64(&s.leaderTerm)
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

			memberURLs, err := s.GetAllMemberURLs()
			if err != nil {
				return err
			}
			strMemberURLs := make(map[string][]string, len(memberURLs))
			for id, us := range memberURLs {
				strMemberURLs[id.String()] = us
			}

			fmt.Fprint(w, "members: ")
			enc := json.NewEncoder(w)
			enc.SetEscapeHTML(false)
			enc.SetIndent("", "  ")
			enc.Encode(strMemberURLs)
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
