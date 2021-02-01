package raft

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"

	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
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
}

type readyHandler struct {
	id refs.PeerID

	storage Storage

	transport *rafthttp.Transport

	raft             Raft
	readyC           chan (<-chan *raft.Ready)
	advanceC         chan struct{}
	fetchingSnapshot int32

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

	r := NewRaft(&c.Config)

	sm := c.StateMachine

	transport := &rafthttp.Transport{
		Logger:      zap.NewNop(),
		ID:          types.ID(id),
		URLs:        types.MustNewURLs([]string{"http://" + id.Addr()}),
		ClusterID:   c.ClusterID,
		Raft:        r,
		ServerStats: stats.NewServerStats(id.String(), id.String()),
		LeaderStats: stats.NewLeaderStats(id.String()),
		ErrorC:      make(chan error, 1),
	}

	if err := transport.Start(); err != nil {
		return nil, err
	}

	executor, err := StartExecutor(r, sm, state.AppliedIndex)
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

		executor:  executor,
		confIndex: state.ConfIndex,

		raftLogger:  logging.GetLogger("ready"),
		eventLogger: logging.GetLogger("event.ready"),
	}

	rc.raftLogger.Info("starting raft instance, applied_index: ", state.AppliedIndex,
		", conf_index: ", state.ConfIndex,
		", conf_state: ", state.ConfState)

	r.Start(node, rc, c.TickDuration)

	rc.Runner = StartRunner(func(stopC <-chan struct{}) error {
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

	for _, peerid := range state.ConfState.Voters {
		peerid := PeerID(peerid)
		if peerid == id {
			continue
		}
		transport.AddPeer(types.ID(peerid), []string{"http://" + peerid.Addr()})
	}

	for _, learnerID := range state.ConfState.Learners {
		learnerID := PeerID(learnerID)
		if learnerID == id {
			continue
		}
		transport.AddPeer(types.ID(learnerID), []string{"http://" + learnerID.Addr()})
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
			case pb.MsgHeartbeat:
				continue
			case pb.MsgHeartbeatResp:
				continue
			}
			msgCnt++
			logFields = append(logFields, (*Message)(&rd.Messages[i]))
		}
	}

	if len(logFields) == 3+msgCnt && len(rd.Messages) > 0 {
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
				rc.transport.AddPeer(types.ID(id),
					[]string{"http://" + peer.Addr()})
			}

			for _, id := range snap.Metadata.ConfState.Learners {
				learnerID := PeerID(id)
				if learnerID == rc.id {
					continue
				}
				rc.transport.AddPeer(types.ID(id),
					[]string{"http://" + learnerID.Addr()})
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

		if err := rc.executor.OnConfState(entry.Index,
			*confState); err != nil {
			return err
		}

		switch typ := cc.Type; typ {
		case pb.ConfChangeAddNode, pb.ConfChangeAddLearnerNode:
			if cc.NodeID != uint64(rc.id) {
				rc.transport.AddPeer(types.ID(cc.NodeID),
					[]string{"http://" + PeerID(cc.NodeID).Addr()})
			}
		case pb.ConfChangeRemoveNode:
			if cc.NodeID != uint64(rc.id) &&
				rc.transport.Get(types.ID(cc.NodeID)) != nil {
				rc.transport.RemovePeer(types.ID(cc.NodeID))
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

	handle, err := rc.raft.Propose(ctx, oplog)
	if err != nil {
		return err
	}

	if err := handle.Wait(ctx); err != nil {
		return err
	}

	return nil
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
