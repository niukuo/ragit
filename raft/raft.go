package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
	"go.uber.org/zap"
)

var ErrStopped = errors.New("stopped")

type msgWithResult struct {
	expectedTerm uint64
	data         []byte
	idxCh        chan<- uint64
	err          error
	resCh        <-chan applyResult
}

type applyResult struct {
	context context.Context
	done    chan<- error
}

// A key-value stream backed by raft
type Node = *raftNode
type raftNode struct {
	id          PeerID
	confChangeC chan pb.ConfChange // proposed cluster config changes
	propC       chan *msgWithResult
	funcC       chan func(node *raft.RawNode)

	// map[index]termAndResult
	doingRequest sync.Map

	lastWAL uint64
	// raft backing for the commit/error channel
	storage   Storage
	executor  Executor
	softState unsafe.Pointer

	transport   *rafthttp.Transport
	serverStats *stats.ServerStats
	leaderStats *stats.LeaderStats

	stopC      chan struct{} // signals proposal channel closed
	raftResult error
	raftDoneC  chan struct{}

	raftLogger  logging.Logger
	eventLogger logging.Logger
}

func RunNode(c Config, peers []PeerID) (Node, error) {

	rpeers := make([]raft.Peer, 0, len(peers))
	for _, peer := range peers {
		rpeers = append(rpeers, raft.Peer{ID: uint64(peer)})
	}

	id := PeerID(c.ID)

	state, err := c.Storage.GetOrInitState(peers)
	if err != nil {
		return nil, err
	}

	lastWAL, err := c.Storage.LastIndex()
	if err != nil {
		return nil, err
	}

	node, err := raft.NewRawNode(&c.Config, nil)
	if err != nil {
		return nil, err
	}

	rc := &raftNode{
		id:          id,
		confChangeC: make(chan pb.ConfChange),
		propC:       make(chan *msgWithResult),
		funcC:       make(chan func(node *raft.RawNode)),

		lastWAL: lastWAL,

		storage: c.Storage,

		serverStats: stats.NewServerStats(id.String(), id.String()),
		leaderStats: stats.NewLeaderStats(id.String()),

		stopC:     make(chan struct{}),
		raftDoneC: make(chan struct{}),

		raftLogger:  logging.GetLogger("raft"),
		eventLogger: logging.GetLogger("event"),
	}

	transport := &rafthttp.Transport{
		Logger:      zap.NewNop(),
		ID:          types.ID(id),
		URLs:        types.MustNewURLs([]string{"http://" + id.Addr()}),
		ClusterID:   c.ClusterID,
		Raft:        rc,
		ServerStats: rc.serverStats,
		LeaderStats: rc.leaderStats,
		ErrorC:      make(chan error),
	}

	executor := NewExecutor(id,
		node, transport, c.StateMachine,
		logging.GetLogger("executor"))

	rc.transport = transport
	rc.executor = executor

	rc.raftLogger.Info("starting raft instance, applied_index: ", state.AppliedIndex,
		", committed_index: ", state.HardState.Commit,
		", conf_index: ", state.ConfIndex,
		", nodes: ", state.Peers)

	if err := rc.executor.Start(
		state.ConfIndex, state.AppliedIndex, state.Peers,
	); err != nil {
		return nil, err
	}

	go func() {
		err := rc.serveRaft(node, c.TickDuration)
		rc.raftResult = err
		close(rc.raftDoneC)
		rc.eventLogger.Warning("node stopped, err: ", err)
	}()
	return rc, nil
}

func reportErrToMsg(msg *msgWithResult, err error) {
	msg.err = err
	close(msg.idxCh)
	if res, ok := <-msg.resCh; ok {
		res.done <- err
	}
}

// applyEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) applyEntries(node *raft.RawNode, ents []pb.Entry) error {
	for i := range ents {
		entry := &ents[i]
		if err := rc.executor.AppendEntry(entry); err != nil {
			return err
		}
	}
	return nil
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) Stop() error {
	close(rc.stopC)
	rc.transport.Stop()
	err := rc.executor.Stop()
	<-rc.raftDoneC
	if e := rc.raftResult; e != nil {
		err = e
	}
	return err
}

var snapshotCatchUpEntriesN uint64 = 10000

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
			logFields = append(logFields, "(", first.Term, ", ", first.Index, ")")
		} else {
			logFields = append(logFields, len(rd.Entries),
				", (", first.Term, ", ", first.Index, ") - (",
				last.Term, ", ", last.Index, ")")
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
			logFields = append(logFields, "(", first.Term, ", ", first.Index, ")")
		} else {
			logFields = append(logFields, len(rd.CommittedEntries),
				", (", first.Term, ", ", first.Index, ") - (",
				last.Term, ", ", last.Index, ")")
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

func (rc *raftNode) serveRaft(node *raft.RawNode, d time.Duration) error {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	confChangeCount := uint64(0)
	// event loop on raft state machine updates

	var lastReady *raft.Ready
	raftState := raft.StateFollower
	var hardState pb.HardState
	leader := PeerID(0)

	for {
		var rd *raft.Ready
		if lastReady != nil {
			rd = lastReady
			lastReady = nil
		} else if node.HasReady() {
			ready := node.Ready()
			rd = &ready
		}
		if rd != nil {
			if fields := readyForLogger(rd); fields != nil {
				rc.raftLogger.Info(fields...)
			}

			if !raft.IsEmptyHardState(rd.HardState) {
				hardState = rd.HardState
			}

			if rd.SoftState != nil {
				atomic.StorePointer(&rc.softState, unsafe.Pointer(rd.SoftState))
				from := raftState
				to := rd.SoftState.RaftState
				rc.eventLogger.Infof("soft state changed from %s to %s, term: %d",
					raftState, rd.SoftState.RaftState, hardState.Term)
				raftState = rd.SoftState.RaftState
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
				objSrcName := leader
				if raftState == raft.StateLeader {
					objSrcName = PeerID(0)
				}
				if err := rc.executor.ApplySnapshot(rd.Snapshot, objSrcName); err != nil {
					return err
				}
				rc.lastWAL = rd.Snapshot.Metadata.Index
			}

			if err := rc.storage.Save(rd.HardState, rd.Entries, rd.MustSync); err != nil {
				return err
			}

			if len(rd.Entries) > 0 {
				rc.lastWAL = rd.Entries[len(rd.Entries)-1].Index
			}
			rc.transport.Send(rd.Messages)
			if err := rc.applyEntries(node, rd.CommittedEntries); err != nil {
				return err
			}
			node.Advance(*rd)
			continue
		}

		select {
		case <-ticker.C:
			node.Tick()

		case cc := <-rc.confChangeC:
			confChangeCount++
			cc.ID = confChangeCount
			node.ProposeConfChange(cc)

		case msg := <-rc.propC:

			if raftState != raft.StateLeader {
				err := fmt.Errorf("state is %s, cant propose", raftState)
				reportErrToMsg(msg, err)
				break
			}

			if msg.expectedTerm != 0 && msg.expectedTerm != hardState.Term {
				err := fmt.Errorf("term not match, expected: %d, actual: %d",
					msg.expectedTerm, hardState.Term)
				reportErrToMsg(msg, err)
				break
			}

			if err := node.Propose(msg.data); err != nil {
				reportErrToMsg(msg, err)
				break
			}

			// hack: just want to know term and index
			if !node.HasReady() {
				return errors.New("proposed but no ready???")
			}

			rd := node.Ready()
			lastReady = &rd

			if len(rd.Entries) != 1 {
				err := fmt.Errorf("got %d entries, dropped", len(rd.Entries))
				reportErrToMsg(msg, err)
				return err
			}

			entry := &rd.Entries[0]

			if entry.Term != hardState.Term {
				return fmt.Errorf("proposed term(%d) not equal to hardstate.term(%d)???",
					entry.Term, hardState.Term)
			}

			rc.executor.AddContext(entry.Term, entry.Index, msg.resCh)
			msg.idxCh <- entry.Index

		case err := <-rc.transport.ErrorC:
			return err

		case fn := <-rc.funcC:
			fn(node)

		case <-rc.stopC:
			return nil
		}
	}
}

func (rc *raftNode) Handler() http.Handler {
	mux := http.NewServeMux()
	rc.InitRouter(mux)
	return mux
}

func (rc *raftNode) InitRouter(mux *http.ServeMux) {
	rh := rc.transport.Handler()
	mux.Handle("/raft", rh)
	mux.Handle("/raft/", rh)
	mux.HandleFunc("/raft/wal", rc.getWAL)
	mux.HandleFunc("/raft/status", rc.getStatus)
	mux.HandleFunc("/raft/server_stat", rc.getServerStat)
	mux.HandleFunc("/raft/leader_stat", rc.getLeaderStat)
	mux.HandleFunc("/raft/members", rc.getMemberStatus)
	mux.HandleFunc("/raft/members/", rc.confChange)
}

func (rc *raftNode) withPipeline(ctx context.Context, fn func(node *raft.RawNode) error) error {
	ch := make(chan error, 1)
	newFn := func(node *raft.RawNode) {
		ch <- fn(node)
		close(ch)
	}
	select {
	case rc.funcC <- newFn:
		return <-ch
	case <-ctx.Done():
		return ctx.Err()
	case <-rc.raftDoneC:
		if rc.raftResult != nil {
			return rc.raftResult
		}
		return ErrStopped
	}
}

func (rc *raftNode) GetStatus(ctx context.Context) (*Status, error) {
	var status Status

	fn := func(node *raft.RawNode) error {
		status = Status(*node.Status())
		return nil
	}

	if err := rc.withPipeline(ctx, fn); err != nil {
		return nil, err
	}

	return &status, nil
}

func (rc *raftNode) getMemberStatus(w http.ResponseWriter, r *http.Request) {
	status, err := rc.GetStatus(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	memberStatus := status.MemberStatus()

	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	encoder.Encode(memberStatus)
}

func (rc *raftNode) confChange(w http.ResponseWriter, r *http.Request) {
	var opType pb.ConfChangeType
	switch r.Method {
	case http.MethodPost:
		opType = pb.ConfChangeAddNode
	case http.MethodDelete:
		opType = pb.ConfChangeRemoveNode
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/raft/members/")
	nodeID, err := ParsePeerID(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	cc := pb.ConfChange{
		Type:   opType,
		NodeID: uint64(nodeID),
	}
	rc.confChangeC <- cc

	// As above, optimistic that raft will apply the conf change
	w.WriteHeader(http.StatusNoContent)
}

func (rc *raftNode) Propose(ctx context.Context, oplog refs.Oplog) error {
	if err := refs.Validate(oplog); err != nil {
		return err
	}

	content, err := proto.Marshal(&oplog)
	if err != nil {
		return err
	}

	idxCh := make(chan uint64, 1)
	resCh := make(chan applyResult) // must no buffer

	msg := msgWithResult{
		data:  content,
		idxCh: idxCh,
		resCh: resCh,
	}

	if v := ctx.Value(CtxExpectedTermKey); v != nil {
		msg.expectedTerm = v.(uint64)
	}

	select {
	case rc.propC <- &msg:
		defer func() { close(resCh) }()
	case <-ctx.Done():
		return ctx.Err()
	case <-rc.raftDoneC:
		if rc.raftResult != nil {
			return rc.raftResult
		}
		return ErrStopped
	}

	var index uint64

	select {
	case i, ok := <-idxCh:
		if !ok {
			err := msg.err
			if err == nil {
				err = errors.New("propose failed")
			}
			return err
		}
		index = i
		defer rc.executor.DeleteContext(index)
	case <-rc.raftDoneC:
		if rc.raftResult != nil {
			return rc.raftResult
		}
		return ErrStopped
	}

	done := make(chan error)

	rc.eventLogger.Info("proposed, index: ", index, ", opcnt: ", len(oplog.Ops),
		", expectedTerm: ", msg.expectedTerm)
	select {
	case resCh <- applyResult{
		context: ctx,
		done:    done,
	}:
		return <-done
	case <-ctx.Done():
		return ctx.Err()
	case <-rc.raftDoneC:
		if rc.raftResult != nil {
			return rc.raftResult
		}
		return ErrStopped
	}
}

func (rc *raftNode) Process(ctx context.Context, m pb.Message) error {
	return rc.withPipeline(ctx, func(node *raft.RawNode) error {
		switch m.Type {
		case pb.MsgHeartbeat:
			if m.Commit > rc.lastWAL {
				msgs := []pb.Message{
					{
						To:   m.From,
						From: uint64(rc.id),
						Type: pb.MsgUnreachable,
					},
					{
						To:   m.From,
						From: uint64(rc.id),
						Type: pb.MsgAppResp, Index: m.Commit,
						Reject: true, RejectHint: rc.lastWAL,
					},
				}
				rc.raftLogger.Warning(
					"maybe my log was lost, try reject index:", m.Commit,
					", message: ", (*Message)(&msgs[1]))
				rc.transport.Send(msgs)
				return nil
			}
		case pb.MsgHeartbeatResp:
		case pb.MsgUnreachable:
			node.ReportUnreachable(m.From)
			rc.raftLogger.Info("report unreachable: ", PeerID(m.From))
			return nil
		default:
			rc.raftLogger.Info("recv message: ", (*Message)(&m))
		}
		if err := node.Step(m); err != nil {
			rc.raftLogger.Warning("step message failed, from: ", PeerID(m.From),
				", to: ", PeerID(m.To), ", err: ", err, ", msg: ", m)
			return err
		}
		return nil
	})
}

func (rc *raftNode) IsIDRemoved(id uint64) bool { return false }

func (rc *raftNode) ReportUnreachable(id uint64) {
	rc.withPipeline(context.Background(), func(node *raft.RawNode) error {
		node.ReportUnreachable(id)
		return nil
	})
}

func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.withPipeline(context.Background(), func(node *raft.RawNode) error {
		node.ReportSnapshot(id, status)
		return nil
	})
}
