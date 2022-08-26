package raft

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/idutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

type ReadyHandler interface {
	Runner

	InitRouter(mux *http.ServeMux)

	ReadyC() chan<- <-chan *raft.Ready
	AdvanceC() <-chan struct{}
	IsFetchingSnapshot() bool
	SetFetchingSnapshot()
	GetLastIndex() (uint64, error)
	GetMemberAddrs(memberID refs.PeerID) ([]string, error)
}

type readIndexState struct {
	triggered chan struct{}

	term     uint64
	id       uint64
	proposed chan struct{}

	index uint64
	err   error
	done  chan struct{}
}

type readyHandler struct {
	id refs.PeerID

	storage Storage

	transport *rafthttp.Transport

	raft             Raft
	readyC           chan (<-chan *raft.Ready)
	advanceC         chan struct{}
	fetchingSnapshot int32

	readIndexIDGen *idutil.Generator
	readIndexNext  unsafe.Pointer
	readIndexDoing *readIndexState

	executor  Executor
	confIndex uint64

	Runner

	raftLogger  logging.Logger
	eventLogger logging.Logger
}

func RunNode(c Config) (Node, error) {

	id := PeerID(c.ID)

	state, err := c.Storage.GetInitState()
	if err != nil {
		return nil, err
	}

	c.Config.DisableProposalForwarding = true
	c.Config.Applied = state.AppliedIndex

	node, err := raft.NewRawNode(&c.Config)
	if err != nil {
		return nil, err
	}

	if c.NewMemberID == nil {
		return nil, errors.New("NewMemberID is nil")
	}

	if len(c.LocalAddrs) == 0 {
		return nil, errors.New("LocalAddrs is empty")
	}

	r := NewRaft(c)

	sm := c.StateMachine

	transport := &rafthttp.Transport{
		Logger:      zap.NewNop(),
		ID:          types.ID(id),
		URLs:        types.MustNewURLs(c.LocalAddrs),
		ClusterID:   c.ClusterID,
		Raft:        r,
		ServerStats: stats.NewServerStats(id.String(), id.String()),
		LeaderStats: stats.NewLeaderStats(id.String()),
		ErrorC:      make(chan error, 1),
	}

	if err := transport.Start(); err != nil {
		return nil, err
	}

	executor, err := StartExecutor(r, sm)
	if err != nil {
		return nil, err
	}

	rc := &readyHandler{
		id: id,

		storage: c.Storage,

		transport: transport,

		raft:     r,
		readyC:   make(chan (<-chan *raft.Ready)),
		advanceC: make(chan struct{}),

		readIndexIDGen: idutil.NewGenerator(uint16(c.ID), time.Now()),
		readIndexNext: unsafe.Pointer(&readIndexState{
			triggered: make(chan struct{}, 1),
			proposed:  make(chan struct{}),
			done:      make(chan struct{}),
		}),

		executor:  executor,
		confIndex: state.ConfIndex,

		raftLogger:  logging.GetLogger("ready"),
		eventLogger: logging.GetLogger("event.ready"),
	}

	rc.raftLogger.Info("starting raft instance, applied_index: ", state.AppliedIndex,
		", conf_index: ", state.ConfIndex,
		", conf_state: ", state.ConfState)

	startChan := make(chan struct{})
	rc.Runner = StartRunner(func(stopC <-chan struct{}) error {
		<-startChan
		e := rc.serveReady(stopC)
		rc.eventLogger.Warning("ready handler stopped, err: ", e)

		rc.executor.Stop()

		rc.transport.Stop()

		rc.raft.Stop()
		<-rc.raft.Done()
		if err := rc.raft.Error(); err != nil {
			e = err
		}

		<-rc.executor.Done()
		if err := rc.executor.Error(); err != nil {
			e = err
		}

		return e
	})

	r.Start(node, rc, c.TickDuration)
	close(startChan)

	for _, peerid := range state.ConfState.Voters {
		peerid := PeerID(peerid)
		if peerid == id {
			continue
		}
		peerAddrs, err := c.Storage.GetMemberAddrs(peerid)
		if err != nil {
			return nil, err
		}
		transport.AddPeer(types.ID(peerid), peerAddrs)
	}

	for _, learnerID := range state.ConfState.Learners {
		learnerID := PeerID(learnerID)
		if learnerID == id {
			continue
		}
		learnerAddrs, err := c.Storage.GetMemberAddrs(learnerID)
		if err != nil {
			return nil, err
		}
		transport.AddPeer(types.ID(learnerID), learnerAddrs)
	}

	return rc, nil
}

func readyForLogger(rd *raft.Ready) []interface{} {
	logFields := []interface{}{
		"ready",
	}

	if rd.SoftState != nil {
		logFields = append(logFields,
			", Lead: ", PeerID(rd.SoftState.Lead),
			", RaftState: ", rd.SoftState.RaftState,
		)
	}

	if !raft.IsEmptyHardState(rd.HardState) {
		logFields = append(logFields,
			", Term: ", rd.HardState.Term,
			", Vote: ", PeerID(rd.HardState.Vote),
			", Commit: ", rd.HardState.Commit,
		)
	}

	if len(rd.Entries) > 0 {
		first := &rd.Entries[0]
		last := &rd.Entries[len(rd.Entries)-1]
		logFields = append(logFields, ", Entries: ")
		if len(rd.Entries) == 1 {
			logFields = append(logFields, first.Term, "/", first.Index)
		} else {
			logFields = append(logFields, len(rd.Entries),
				", [", first.Term, "/", first.Index, ", ",
				last.Term, "/", last.Index, "]")
		}
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		logFields = append(logFields,
			", Snapshot: ",
			(*Snapshot)(&rd.Snapshot),
		)
	}

	if len(rd.CommittedEntries) > 0 {
		first := &rd.CommittedEntries[0]
		last := &rd.CommittedEntries[len(rd.CommittedEntries)-1]
		logFields = append(logFields, ", Committed: ")
		if len(rd.CommittedEntries) == 1 {
			logFields = append(logFields, "(", first.Term, "/", first.Index, ")")
		} else {
			logFields = append(logFields, len(rd.CommittedEntries),
				", (", first.Term, "/", first.Index, ") - (",
				last.Term, "/", last.Index, ")")
		}
	}

	msgCnt := 0

	if len(rd.Messages) > 0 {
		logFields = append(logFields, ", Messages: ", len(rd.Messages))
		for i := range rd.Messages {
			switch rd.Messages[i].Type {
			case pb.MsgHeartbeat, pb.MsgHeartbeatResp:
				continue
			case pb.MsgReadIndex, pb.MsgReadIndexResp:
				continue
			}
			msgCnt++
			logFields = append(logFields, (*Message)(&rd.Messages[i]))
		}
	}

	if len(logFields) == 3+msgCnt && len(rd.Messages) > 0 {
		return nil
	}

	if len(logFields) == 1 && len(rd.ReadStates) > 0 {
		return nil
	}

	return logFields
}

func (rc *readyHandler) serveReady(stopC <-chan struct{}) error {
	raftState := raft.StateFollower
	leader := PeerID(0)

	var hardState pb.HardState

	for {
		var rd *raft.Ready

		var readIndexC <-chan struct{}
		if rc.readIndexDoing == nil {
			readIndexC = (*readIndexState)(rc.readIndexNext).triggered
		}

		select {
		case <-stopC:
			return nil

		case err := <-rc.transport.ErrorC:
			rc.eventLogger.Warning("transport stopped, err: ", err)
			return err

		case <-rc.executor.Done():
			rc.eventLogger.Warning("executor stopped unexpectedly, err: ",
				rc.executor.Error())
			return nil

		case <-readIndexC:
			rstate := (*readIndexState)(rc.readIndexNext)

			newState := &readIndexState{
				triggered: make(chan struct{}, 1),
				proposed:  make(chan struct{}),
				done:      make(chan struct{}),
			}
			atomic.StorePointer(&rc.readIndexNext, unsafe.Pointer(newState))

			rstate.id = rc.readIndexIDGen.Next()
			rstate.term = hardState.Term
			close(rstate.proposed)
			var rctx [8]byte
			binary.BigEndian.PutUint64(rctx[:], rstate.id)
			if err := rc.raft.proposeReadIndex(context.TODO(), rctx[:]); err != nil {
				rstate.err = err
				close(rstate.done)
			} else {
				rc.readIndexDoing = rstate
			}
			continue

		case ch := <-rc.readyC:
			select {
			case rd = <-ch:
			case <-stopC:
				return nil
			}
		}

		if fields := readyForLogger(rd); fields != nil {
			rc.raftLogger.Info(fields...)
		}

		if !raft.IsEmptyHardState(rd.HardState) {
			hardState = rd.HardState
		}

		if rd.SoftState != nil {
			from := raftState
			to := rd.SoftState.RaftState
			raftState = to
			leader = PeerID(rd.SoftState.Lead)
			if from != raft.StateLeader && to == raft.StateLeader {
				rc.executor.OnLeaderStart(hardState.Term)
			} else if from == raft.StateLeader && to != raft.StateLeader {
				rc.executor.OnLeaderStop()
			}
		}

		for _, rstate := range rd.ReadStates {
			id := binary.BigEndian.Uint64(rstate.RequestCtx)
			if rc.readIndexDoing != nil && rc.readIndexDoing.id == id {
				state := rc.readIndexDoing
				rc.readIndexDoing = nil
				state.index = rstate.Index
				close(state.done)
			} else {
				rc.raftLogger.Info("read index",
					", id: ", id,
					", index: ", rstate.Index,
				)
			}
		}

		if !raft.IsEmptyHardState(rd.HardState) {
			if rc.readIndexDoing != nil &&
				rc.readIndexDoing.term != hardState.Term {
				state := rc.readIndexDoing
				rc.readIndexDoing = nil
				state.err = errors.New("term changed")
				close(state.done)
				rc.raftLogger.Warning("read index term changed",
					", id: ", strconv.FormatUint(state.id, 16),
					", term: ", state.term,
					", new_term: ", hardState.Term,
				)
			}
		}

		// save snapshot first. if HardState.Commit is saved and snapshot apply failed,
		// it will panic on next start.
		if !raft.IsEmptySnap(rd.Snapshot) {
			snap := rd.Snapshot
			objSrcName := leader
			if objSrcName == rc.id {
				objSrcName = 0
			}

			if err := rc.executor.OnSnapshot(rd.Snapshot, objSrcName); err != nil {
				rc.raftLogger.Warning("apply snapshot failed, err: ", err)
				return err
			}

			rc.transport.RemoveAllPeers()
			for _, id := range snap.Metadata.ConfState.Voters {
				peer := PeerID(id)
				if peer == rc.id {
					continue
				}
				peerAddrs, err := rc.storage.GetMemberAddrs(peer)
				if err != nil {
					return err
				}
				rc.transport.AddPeer(types.ID(id), peerAddrs)
			}

			for _, id := range snap.Metadata.ConfState.Learners {
				learnerID := PeerID(id)
				if learnerID == rc.id {
					continue
				}
				learnerAddrs, err := rc.storage.GetMemberAddrs(learnerID)
				if err != nil {
					return err
				}
				rc.transport.AddPeer(types.ID(id), learnerAddrs)
			}

			atomic.StoreUint64(&rc.confIndex, snap.Metadata.Index)
		}
		atomic.StoreInt32(&rc.fetchingSnapshot, 0)

		if err := rc.storage.Save(rd.HardState, rd.Entries); err != nil {
			rc.raftLogger.Warning("persistent failed, err: ", err)
			return err
		}

		rc.transport.Send(rd.Messages)

		for i := range rd.CommittedEntries {
			entry := &rd.CommittedEntries[i]
			if err := rc.applyEntry(entry); err != nil {
				return err
			}
		}

		select {
		case rc.advanceC <- struct{}{}:
		case <-stopC:
			return nil
		}
	}
}

func (rc *readyHandler) applyEntry(entry *pb.Entry) error {
	switch typ := entry.Type; typ {
	case pb.EntryNormal:
		if err := rc.executor.OnEntry(entry); err != nil {
			rc.raftLogger.Warning("append entry failed, index: ", entry.Index, ", err: ", err)
			return err
		}
	case pb.EntryConfChange:

		if entry.Index <= rc.confIndex {
			rc.storage.OnConfIndexChange(entry.Index)
			break
		}

		var cc pb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return err
		}

		confState, err := rc.raft.applyConfChange(cc)
		if err != nil {
			return err
		}

		members, err := getChangeMembers(cc)
		if err != nil {
			return err
		}

		if err := rc.executor.OnConfState(entry.Index,
			*confState, members, cc.Type); err != nil {
			return err
		}

		switch typ := cc.Type; typ {
		case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode:
			if cc.NodeID != uint64(rc.id) {
				for _, m := range members {
					rc.transport.AddPeer(types.ID(cc.NodeID), m.PeerAddrs)
					rc.raftLogger.Infof("transport.AddPeer of id %v", m.PeerAddrs)
				}
			}
		case pb.ConfChangeRemoveNode:
			if cc.NodeID == uint64(rc.id) {
				rc.mayTransferLeader()
			} else if rc.transport.Get(types.ID(cc.NodeID)) != nil {
				rc.transport.RemovePeer(types.ID(cc.NodeID))
				rc.raftLogger.Infof("transport.RemovePeer of id %s", cc.NodeID)
			}
		default:
			return fmt.Errorf("unsupported conf change type %s", typ)
		}

		atomic.StoreUint64(&rc.confIndex, entry.Index)
	default:
		return fmt.Errorf("unsupported entry type %s", typ)
	}

	return nil
}

func getChangeMembers(cc pb.ConfChange) ([]*refs.Member, error) {
	var m refs.Member
	if cc.Type == pb.ConfChangeRemoveNode {
		if len(cc.Context) != 0 {
			return nil, errors.New("remove node cc context not nil")
		}
		m = refs.Member{
			ID: refs.PeerID(cc.NodeID),
		}
	} else {
		dec := json.NewDecoder(strings.NewReader(string(cc.Context)))
		dec.DisallowUnknownFields()
		err := dec.Decode(&m)
		if err != nil {
			return nil, err
		}
	}
	members := []*refs.Member{
		&m,
	}
	return members, nil
}

func (rc *readyHandler) mayTransferLeader() {
	fn := func(node *raft.RawNode) error {
		status := node.Status()
		if status.Lead != uint64(rc.id) {
			return nil
		}
		rc.raftLogger.Infof("the leader %s is deleted, need to transfer leader", rc.id)
		var transferee uint64
		for id, progress := range status.Progress {
			if id == uint64(rc.id) || progress.IsLearner {
				continue
			}
			if progress.Match == status.Commit {
				transferee = id
				break
			}
		}
		if transferee == 0 {
			rc.raftLogger.Infof("has no match node, not change leader")
			return nil
		}

		rc.raftLogger.Infof("to transfer leader to %s", transferee)
		node.TransferLeader(transferee)
		return nil
	}
	rc.raft.withPipeline(context.Background(), fn)
}

func (rc *readyHandler) ReadyC() chan<- <-chan *raft.Ready {
	return rc.readyC
}

func (rc *readyHandler) AdvanceC() <-chan struct{} {
	return rc.advanceC
}

func (rc *readyHandler) IsFetchingSnapshot() bool {
	return atomic.LoadInt32(&rc.fetchingSnapshot) != 0
}

func (rc *readyHandler) SetFetchingSnapshot() {
	atomic.StoreInt32(&rc.fetchingSnapshot, 1)
}

func (rc *readyHandler) GetLastIndex() (uint64, error) {
	return rc.storage.LastIndex()
}

func (rc *readyHandler) GetMemberAddrs(memberID refs.PeerID) ([]string, error) {
	return rc.storage.GetMemberAddrs(memberID)
}

func (rc *readyHandler) Handler() http.Handler {
	mux := http.NewServeMux()
	rc.InitRouter(mux)
	return mux
}

func (rc *readyHandler) InitRouter(mux *http.ServeMux) {
	rh := rc.transport.Handler()
	mux.Handle("/raft", rh)
	mux.Handle("/raft/", rh)
	mux.HandleFunc("/raft/status", rc.getStatus)
	mux.HandleFunc("/raft/read_index", rc.getReadIndex)
	mux.HandleFunc("/raft/wal", rc.getWAL)
	mux.HandleFunc("/raft/snapshot", rc.getSnapshot)
	mux.HandleFunc("/raft/server_stat", rc.getServerStat)
	mux.HandleFunc("/raft/leader_stat", rc.getLeaderStat)
	rc.raft.InitRouter(mux)
}

func (rc *readyHandler) getSnapshot(w http.ResponseWriter, r *http.Request) {
	snapshot, err := rc.storage.Snapshot()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	pb, err := snapshot.Marshal()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-protobuf")
	w.Header().Set("Content-Length", strconv.Itoa(len(pb)))
	w.Write(pb)
}

func (rc *readyHandler) Propose(ctx context.Context, oplog refs.Oplog) error {

	start := time.Now()

	handle, err := rc.raft.Propose(ctx, oplog)
	if err != nil {
		return err
	}

	if err := handle.Wait(ctx); err != nil {
		return err
	}

	proposeSeconds.Observe(time.Since(start).Seconds())
	proposeCounter.Inc()

	proposePackBytes.Observe(float64(len(oplog.GetObjPack())))

	return nil
}

func (rc *readyHandler) proposeReadIndex() *readIndexState {

	state := (*readIndexState)(atomic.LoadPointer(&rc.readIndexNext))
	select {
	case state.triggered <- struct{}{}:
	default:
	}

	return state
}

func (rc *readyHandler) ReadIndex(ctx context.Context) (uint64, error) {

	state := rc.proposeReadIndex()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-state.done:
		if err := state.err; err != nil {
			return 0, err
		}
		return state.index, nil
	case <-rc.Runner.Done():
		return 0, ErrStopped
	}
}

func (rc *readyHandler) GetStatus(ctx context.Context) (*Status, error) {
	var status Status

	fn := func(node *raft.RawNode) error {
		status = Status(node.Status())
		return nil
	}

	if err := rc.raft.withPipeline(ctx, fn); err != nil {
		return nil, err
	}

	return &status, nil
}
