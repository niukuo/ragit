package raft

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/pkg/v3/idutil"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.uber.org/zap"
)

type ReadyHandler interface {
	Runner

	InitRouter(mux *http.ServeMux)

	ReadyC() chan<- <-chan *raft.Ready
	AdvanceC() <-chan struct{}
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

	raft     Raft
	readyC   chan (<-chan *raft.Ready)
	advanceC chan struct{}

	readIndexIDGen *idutil.Generator
	readIndexNext  unsafe.Pointer
	// if readIndexTimer is running, readIndexDoing is always not nil
	readIndexDoing   *readIndexState
	readIndexTimer   *time.Timer
	readIndexTimeout time.Duration

	txnLocker MapLocker

	executor  Executor
	confIndex uint64

	channel *Channel
	ClusterServer
	MaintenanceServer

	Runner

	raftLogger  logging.Logger
	eventLogger logging.Logger
}

func RunNode(c Config,
	opts ...NodeOptions,
) (Node, error) {

	if len(c.PeerListenURLs) == 0 {
		return nil, errors.New("PeerListenURLs cannot be empty")
	}

	state, err := c.Storage.GetInitState()
	if err != nil {
		return nil, err
	}

	if c.ID != 0 && c.ID != state.LocalID {
		return nil, fmt.Errorf("local id mismatch, expected: %s, actual: %s",
			PeerID(state.LocalID), types.ID(c.ID))
	}

	id := PeerID(state.LocalID)

	c.Config.ID = state.LocalID
	c.Config.DisableProposalForwarding = true
	c.Config.Applied = state.AppliedIndex

	node, err := raft.NewRawNode(&c.Config)
	if err != nil {
		return nil, err
	}

	r := NewRaft(c, opts...)

	sm := c.StateMachine

	transport := &rafthttp.Transport{
		Logger:      zap.NewNop(),
		ID:          types.ID(id),
		URLs:        types.MustNewURLs(c.PeerListenURLs),
		ClusterID:   c.ClusterID,
		Raft:        r,
		ServerStats: stats.NewServerStats(id.String(), id.String()),
		LeaderStats: stats.NewLeaderStats(nil, id.String()),
		ErrorC:      make(chan error, 1),
	}

	channel := NewChannel(c.Storage)

	serverConfig := ServerConfig{
		clusterId: c.ClusterID,
		id:        id,

		storage: c.Storage,
		raft:    r,
		channel: channel,

		newMemberID: refs.DefaultNewMemberID,
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
		readIndexTimeout: 2 * time.Second,

		txnLocker: NewMapLocker(),

		confIndex: state.ConfIndex,

		channel:           channel,
		ClusterServer:     NewClusterServer(serverConfig),
		MaintenanceServer: NewMaintenanceServer(serverConfig),

		raftLogger:  logging.GetLogger("ready"),
		eventLogger: logging.GetLogger("event.ready"),
	}

	for _, opt := range opts {
		opt.applyReadyHandler(rc)
	}

	if err := transport.Start(); err != nil {
		return nil, err
	}

	executor, err := StartExecutor(r, sm)
	if err != nil {
		return nil, err
	}
	rc.executor = executor

	rc.raftLogger.Info("starting raft instance, applied_index: ", state.AppliedIndex,
		", conf_index: ", state.ConfIndex,
		", conf_state: ", state.ConfState)

	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	rc.readIndexTimer = timer

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

		rc.channel.Close()

		<-rc.executor.Done()
		if err := rc.executor.Error(); err != nil {
			e = err
		}

		return e
	})

	r.Start(node, rc, c.TickDuration)
	close(startChan)

	memberURLs := make(map[PeerID][]string, len(state.Members))
	for _, m := range state.Members {
		memberURLs[m.ID] = m.PeerURLs
	}

	for _, peerID := range state.ConfState.Voters {
		peerID := PeerID(peerID)
		if peerID == id {
			continue
		}
		us, ok := memberURLs[peerID]
		if !ok {
			return nil, fmt.Errorf("voter: %s not found in members: %v",
				peerID, memberURLs)
		}
		transport.AddPeer(types.ID(peerID), us)
	}

	for _, learnerID := range state.ConfState.Learners {
		learnerID := PeerID(learnerID)
		if learnerID == id {
			continue
		}
		us, ok := memberURLs[learnerID]
		if !ok {
			return nil, fmt.Errorf("learner: %s not found in members: %v",
				learnerID, memberURLs)
		}
		transport.AddPeer(types.ID(learnerID), us)
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
				rc.readIndexStart(rstate, rc.readIndexTimeout)
			}
			continue

		case <-rc.readIndexTimer.C:
			rc.readIndexClear(errors.New("readIndex timeout"), false)
			continue

		case ch := <-rc.readyC:
			select {
			case rd = <-ch:
			case <-stopC:
				return nil
			}
		}

		start := time.Now()

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
				leaderChanges.Inc()
				rc.executor.OnLeaderStart(hardState.Term)
			} else if from == raft.StateLeader && to != raft.StateLeader {
				rc.executor.OnLeaderStop()
			}

			if leader != PeerID(0) && to == raft.StateFollower {
				rc.channel.Update(leader)
			} else {
				rc.channel.ResetConn()
			}
		}

		for _, rstate := range rd.ReadStates {
			id := binary.BigEndian.Uint64(rstate.RequestCtx)
			if rc.readIndexDoing != nil && rc.readIndexDoing.id == id {
				rc.readIndexFire(rstate.Index)
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
				rc.readIndexClear(&errTermChanged{
					expTerm: state.term,
					curTerm: hardState.Term,
				}, true)
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

			snapshotData, err := refs.DecodeSnapshot(rd.Snapshot.Data)
			if err != nil {
				return err
			}
			memberURLs := make(map[PeerID][]string, len(snapshotData.Members))
			for _, m := range snapshotData.Members {
				memberURLs[m.ID] = m.PeerURLs
			}

			for _, id := range snap.Metadata.ConfState.Voters {
				peerID := PeerID(id)
				if peerID == rc.id {
					continue
				}
				us, ok := memberURLs[peerID]
				if !ok {
					return fmt.Errorf("voter: %s not found in members: %v",
						peerID, memberURLs)
				}
				rc.transport.AddPeer(types.ID(id), us)
			}

			for _, id := range snap.Metadata.ConfState.Learners {
				learnerID := PeerID(id)
				if learnerID == rc.id {
					continue
				}
				us, exist := memberURLs[learnerID]
				if !exist {
					return fmt.Errorf("learner: %s not found in members: %v",
						learnerID, memberURLs)
				}
				rc.transport.AddPeer(types.ID(id), us)
			}

			atomic.StoreUint64(&rc.confIndex, snap.Metadata.Index)
		}

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

		serveReadySeconds.Observe(time.Since(start).Seconds())

		select {
		case rc.advanceC <- struct{}{}:
		case <-stopC:
			return nil
		}
	}
}

func (rc *readyHandler) readIndexStart(state *readIndexState, d time.Duration) {
	rc.readIndexDoing = state
	rc.readIndexTimer.Reset(d)
}

func (rc *readyHandler) readIndexFire(index uint64) {
	state := rc.readIndexDoing
	rc.readIndexDoing = nil
	if !rc.readIndexTimer.Stop() {
		select {
		case <-rc.readIndexTimer.C:
		default:
			panic("readIndexTimer stop failed")
		}
	}
	state.index = index
	close(state.done)
}

func (rc *readyHandler) readIndexClear(err error, stop bool) {
	state := rc.readIndexDoing
	rc.readIndexDoing = nil
	if stop && !rc.readIndexTimer.Stop() {
		select {
		case <-rc.readIndexTimer.C:
		default:
			panic("readIndexTimer stop failed")
		}
	}
	state.err = err
	close(state.done)
}

func (rc *readyHandler) applyEntry(entry *pb.Entry) error {
	switch typ := entry.Type; typ {
	case pb.EntryNormal:
		if err := rc.executor.OnEntry(entry); err != nil {
			rc.raftLogger.Warning("append entry failed, index: ", entry.Index, ", err: ", err)
			return err
		}
	case pb.EntryConfChange:

		// index and conf_index are stored in different places, and we use the index as AppliedIndex for raft restart.
		// so we need to skip the conf change entry when the index is less than the conf_index.
		if entry.Index <= rc.confIndex {
			rc.storage.OnConfIndexChange(entry.Index)
			break
		}

		var cc pb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return err
		}

		applyConfChange := func() error {
			req, err := rc.raft.getContext(entry.Term, entry.Index)
			if err != nil {
				return err
			}

			if req != nil {
				defer func() { req.fire(err) }()
			}

			members, err := rc.checkAndGetChangeMembers(cc)
			if err != nil {
				// The configuration change may be cancelled at this point
				// by setting the NodeID field to zero before calling ApplyConfChange
				cc.NodeID = raft.None
				rc.raft.applyConfChange(cc)
				rc.storage.OnConfIndexChange(entry.Index)
				return nil
			}

			if len(members) > 1 {
				return fmt.Errorf("got more than 1 members: %v", members)
			}

			confState, err := rc.raft.applyConfChange(cc)
			if err != nil {
				return err
			}

			if err = rc.executor.OnConfState(entry.Index,
				*confState, members, cc.Type); err != nil {
				return err
			}

			switch typ := cc.Type; typ {
			case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode:
				if cc.NodeID != uint64(rc.id) && len(members) == 1 {
					rc.transport.AddPeer(types.ID(cc.NodeID), members[0].PeerURLs)
					rc.raftLogger.Infof("transport.AddPeer of id %s, peer_urls: %v",
						types.ID(cc.NodeID), members[0].PeerURLs)
				}
			case pb.ConfChangeUpdateNode:
				if cc.NodeID != uint64(rc.id) && len(members) == 1 {
					rc.transport.UpdatePeer(types.ID(cc.NodeID), members[0].PeerURLs)
					rc.raftLogger.Infof("transport.UpdatePeer of id %s, peer_urls: %v",
						types.ID(cc.NodeID), members[0].PeerURLs)
				}
			case pb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					rc.mayTransferLeader()
					return fmt.Errorf("self %s has been removed from cluster, to stop", rc.id)
				} else if rc.transport.Get(types.ID(cc.NodeID)) != nil {
					rc.transport.RemovePeer(types.ID(cc.NodeID))
					rc.raftLogger.Infof("transport.RemovePeer of id %s", types.ID(cc.NodeID))
				}
			default:
				return fmt.Errorf("unsupported conf change type %s", typ)
			}

			return nil
		}

		if err := applyConfChange(); err != nil {
			return err
		}

		atomic.StoreUint64(&rc.confIndex, entry.Index)
	default:
		return fmt.Errorf("unsupported entry type %s", typ)
	}

	return nil
}

func (rc *readyHandler) checkAndGetChangeMembers(cc pb.ConfChange) ([]refs.Member, error) {

	id := refs.PeerID(cc.NodeID)

	memberURLs, err := rc.storage.GetAllMemberURLs()
	if err != nil {
		return nil, err
	}

	var members []refs.Member

	switch cc.Type {
	case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode:

		if len(cc.Context) == 0 {
			return nil, errors.New("ConfChangeAdd(Learner)Node Context cannot be empty")
		}

		var confChangeContext ConfChangeContext
		if err := json.Unmarshal(cc.Context, &confChangeContext); err != nil {
			return nil, err
		}

		if confChangeContext.IsPromote {

			if cc.Type != pb.ConfChangeAddNode {
				return nil, fmt.Errorf("promote: type mismatch: %s", cc.Type)
			}
			if _, ok := memberURLs[id]; !ok {
				return nil, fmt.Errorf("member id: %s not found", id)
			}

			confState, err := rc.storage.GetConfState()
			if err != nil {
				return nil, err
			}

			isLearner := false
			for _, learnerID := range confState.Learners {
				if id == PeerID(learnerID) {
					isLearner = true
					break
				}
			}

			if !isLearner {
				return nil, fmt.Errorf("can only promote a learner member: %s", id)
			}

		} else {
			if _, ok := memberURLs[id]; ok {
				return nil, fmt.Errorf("member id: %s already exist", id)
			}

			urls := make(map[string]bool)
			for _, us := range memberURLs {
				for _, u := range us {
					urls[u] = true
				}
			}

			for _, u := range confChangeContext.PeerURLs {
				if urls[u] {
					return nil, fmt.Errorf("peerURL: %s already exist", u)
				}
			}

			members = append(members, confChangeContext.Member)
		}

	case pb.ConfChangeUpdateNode:
		if len(cc.Context) == 0 {
			return nil, errors.New("ConfChangeUpdateNode Context cannot be empty")
		}

		var confChangeContext ConfChangeContext
		if err := json.Unmarshal(cc.Context, &confChangeContext); err != nil {
			return nil, err
		}

		if _, ok := memberURLs[id]; !ok {
			return nil, fmt.Errorf("member id: %s not found", id)
		}

		members = append(members, refs.NewMember(id, confChangeContext.PeerURLs))

	case pb.ConfChangeRemoveNode:
		if len(cc.Context) != 0 {
			return nil, fmt.Errorf("ConfChangeRemoveNode Context: %s should be empty", cc.Context)
		}

		if _, ok := memberURLs[id]; !ok {
			return nil, fmt.Errorf("member id: %s not found", id)
		}

		members = append(members, refs.NewMember(id, nil))

	default:
		return nil, fmt.Errorf("unsupported conf change type: %s", cc.Type)
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

func (rc *readyHandler) GetAllMemberURLs() (map[PeerID][]string, error) {
	return rc.storage.GetAllMemberURLs()
}

func (rc *readyHandler) GetURLsByMemberID(id PeerID) ([]string, error) {
	return rc.storage.GetURLsByMemberID(id)
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
	mux.HandleFunc("/raft/forward_stat", rc.getForwardStat)
	mux.HandleFunc("/raft/members", rc.getMemberStatus)
	rc.raft.InitRouter(mux)
}

func (rc *readyHandler) ClusterService() ClusterServer {
	return rc.ClusterServer
}

func (rc *readyHandler) MaintenanceService() MaintenanceServer {
	return rc.MaintenanceServer
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

func (rc *readyHandler) Propose(ctx context.Context, cmds []*packp.Command, pack []byte, handle refs.ReqHandle) (DoingRequest, error) {

	term := rc.storage.GetLeaderTerm()
	if term == 0 {
		return nil, errors.New("not leader")
	}

	// make a sorted copy
	sortedCmds := append(make([]*packp.Command, 0, len(cmds)), cmds...)
	sort.Slice(sortedCmds, func(i, j int) bool {
		return sortedCmds[i].Name < sortedCmds[j].Name
	})

	unlocker := func() {}
	defer func() {
		if unlocker != nil {
			unlocker()
		}
	}()

	for _, cmd := range sortedCmds {
		if err := cmd.Name.Validate(); err != nil {
			return nil, fmt.Errorf("invalid ref format: %s, err: %w", cmd.Name, err)
		}
		u, err := rc.txnLocker.Lock(ctx, cmd.Name)
		if err != nil {
			return nil, fmt.Errorf("lock ref %s failed, err: %w", cmd.Name, err)
		}
		u2 := unlocker
		unlocker = func() {
			u()
			u2()
		}
	}

	allRefs, err := rc.storage.GetAllRefs()
	if err != nil {
		return nil, err
	}

	for _, cmd := range cmds {
		if hash := allRefs[string(cmd.Name)]; hash != refs.Hash(cmd.Old) {
			return nil, fmt.Errorf("ref not match, %s expect: %s, actual: %s",
				cmd.Name, cmd.Old, hash)
		}
	}

	if t := rc.storage.GetLeaderTerm(); t == 0 {
		return nil, fmt.Errorf("leader lost after lock, term: %d", term)
	} else if t != term {
		// storage may updated during leader change
		rc.raftLogger.Warning("term changed",
			", before: ", term,
			", now: ", t,
		)
		return nil, fmt.Errorf("term changed from %d to %d, need retry", term, t)
	}

	ctx = WithExpectedTerm(ctx, term)
	u := unlocker
	ctx = WithReqDoneCallback(ctx, func(err error) { u() })

	req, err := rc.raft.Propose(ctx, cmds, pack, handle)
	if err != nil {
		return nil, err
	}
	unlocker = nil

	return req, nil
}

func (rc *readyHandler) proposeReadIndex() *readIndexState {

	state := (*readIndexState)(atomic.LoadPointer(&rc.readIndexNext))
	select {
	case state.triggered <- struct{}{}:
	default:
	}

	return state
}

func (rc *readyHandler) getMemberStatus(w http.ResponseWriter, r *http.Request) {
	var memberStatus *MemberStatus

	if err := rc.raft.withPipeline(r.Context(), func(node *raft.RawNode) error {
		var errStatus error
		memberStatus, errStatus = (Status)(node.Status()).MemberStatus(rc.GetAllMemberURLs)
		if errStatus != nil {
			return errStatus
		}
		return nil
	}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	encoder.Encode(memberStatus)
}

func (rc *readyHandler) ReadIndex(ctx context.Context) (uint64, error) {

	readIndexCounter.Inc()

	start := time.Now()
	defer func() { readIndexSeconds.Observe(time.Since(start).Seconds()) }()

	state := rc.proposeReadIndex()

	select {
	case <-ctx.Done():
		readIndexFailedCounter.Inc()
		return 0, ctx.Err()
	case <-state.done:
		if err := state.err; err != nil {
			readIndexFailedCounter.Inc()
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

func (rc *readyHandler) BeginTx(initer TxIniter) (*Tx, error) {

	term := rc.storage.GetLeaderTerm()
	if term == 0 {
		return nil, errors.New("not leader")
	}

	refsMap, insert, unlocker, err := initer(rc.txnLocker, rc.storage)
	if err != nil {
		return nil, err
	}
	defer func() {
		if unlocker != nil {
			unlocker()
		}
	}()

	if t := rc.storage.GetLeaderTerm(); t == 0 {
		return nil, fmt.Errorf("leader lost after lock, term: %d", term)
	} else if t != term {
		// storage may updated during leader change
		rc.raftLogger.Warning("term changed",
			", before: ", term,
			", now: ", t,
		)
		return nil, fmt.Errorf("term changed from %d to %d, need retry", term, t)
	}

	cmdMap := make(map[plumbing.ReferenceName]*packp.Command)
	for refName, hash := range refsMap {
		cmdMap[refName] = &packp.Command{
			Name: refName,
			Old:  hash,
			New:  hash,
		}
	}

	stx := newTx(rc.raft, term, insert, unlocker, cmdMap)
	unlocker = nil

	return stx, nil
}

func LockGlobal(
	ctx context.Context,
	txnLocker MapLocker,
	lockErr error,
	storage Storage) (
	map[plumbing.ReferenceName]plumbing.Hash,
	bool,
	Unlocker,
	error) {

	if lockErr != nil {
		if err := txnLocker.LockGlobalWithError(ctx, lockErr); err != nil {
			return nil, false, nil, err
		}
	} else {
		if err := txnLocker.LockGlobal(ctx); err != nil {
			return nil, false, nil, err
		}
	}

	unlocker := txnLocker.UnlockGlobal
	defer func() {
		if unlocker != nil {
			unlocker()
		}
	}()

	allRefs, err := storage.GetAllRefs()
	if err != nil {
		return nil, false, nil, err
	}

	cmds := map[plumbing.ReferenceName]plumbing.Hash{}

	for refName, hash := range allRefs {
		refName := plumbing.ReferenceName(refName)
		cmds[plumbing.ReferenceName(refName)] = plumbing.Hash(hash)
	}

	u := unlocker
	unlocker = nil

	return cmds, true, u, nil
}

func LockRefList(
	ctx context.Context,
	txnLocker MapLocker,
	storage Storage,
	refName plumbing.ReferenceName, refNames ...plumbing.ReferenceName) (
	map[plumbing.ReferenceName]plumbing.Hash,
	bool,
	Unlocker,
	error) {

	unlocker, err := txnLocker.Lock(ctx, refName)
	if err != nil {
		return nil, false, nil, err
	}
	defer func() {
		if unlocker != nil {
			unlocker()
		}
	}()

	for _, refName := range refNames {
		u, err := txnLocker.Lock(ctx, refName)
		if err != nil {
			unlocker()
			return nil, false, nil, fmt.Errorf("lock ref %s failed, err: %w", refName, err)
		}
		u2 := unlocker
		unlocker = func() {
			u()
			u2()
		}
	}

	allRefs, err := storage.GetAllRefs()
	if err != nil {
		return nil, false, nil, err
	}

	cmds := map[plumbing.ReferenceName]plumbing.Hash{
		refName: plumbing.Hash(allRefs[string(refName)]),
	}

	for _, refName := range refNames {
		cmds[refName] = plumbing.Hash(allRefs[string(refName)])
	}

	u := unlocker
	unlocker = nil

	return cmds, false, u, nil
}
