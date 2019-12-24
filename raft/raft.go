package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
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
	id PeerID

	readyC   chan (<-chan *raft.Ready)
	advanceC chan struct{}

	confChangeC chan pb.ConfChange // proposed cluster config changes
	propC       chan *msgWithResult
	funcC       chan func(node *raft.RawNode)

	lastWAL          uint64
	fetchingSnapshot bool

	storage   Storage
	executor  Executor
	softState unsafe.Pointer

	transport   *rafthttp.Transport
	serverStats *stats.ServerStats
	leaderStats *stats.LeaderStats

	stopC chan struct{} // signals proposal channel closed

	readyResult error
	readyDoneC  chan struct{}

	raftResult error
	raftDoneC  chan struct{}

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

	node, err := raft.NewRawNode(&c.Config, nil)
	if err != nil {
		return nil, err
	}

	rc := &raftNode{
		id: id,

		readyC:   make(chan (<-chan *raft.Ready)),
		advanceC: make(chan struct{}),

		confChangeC: make(chan pb.ConfChange),
		propC:       make(chan *msgWithResult),
		funcC:       make(chan func(node *raft.RawNode)),

		storage: c.Storage,

		serverStats: stats.NewServerStats(id.String(), id.String()),
		leaderStats: stats.NewLeaderStats(id.String()),

		stopC: make(chan struct{}),

		readyDoneC: make(chan struct{}),

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

	go func() {
		err := rc.serveReady()
		rc.readyResult = err
		close(rc.readyDoneC)
		rc.eventLogger.Warning("ready handler stopped, err: ", err)
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
func (rc *raftNode) applyEntries(ents []pb.Entry) error {
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

func (rc *raftNode) serveReady() error {
	raftState := raft.StateFollower
	var hardState pb.HardState
	leader := PeerID(0)
	for {
		var rd *raft.Ready
		select {
		case <-rc.stopC:
			return nil
		case ch := <-rc.readyC:
			rd = <-ch
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
			rc.fetchingSnapshot = false
		}

		if err := rc.storage.Save(rd.HardState, rd.Entries, rd.MustSync); err != nil {
			return err
		}

		rc.transport.Send(rd.Messages)
		if err := rc.applyEntries(rd.CommittedEntries); err != nil {
			return err
		}

		select {
		case rc.advanceC <- struct{}{}:
		case <-rc.stopC:
			return nil
		}
	}
}

func (rc *raftNode) serveRaft(node *raft.RawNode, d time.Duration) error {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	confChangeCount := uint64(0)
	// event loop on raft state machine updates

	var hardState pb.HardState
	raftState := raft.StateFollower
	var handlingReady raft.Ready
	var advanceC <-chan struct{}
	readyCh := make(chan *raft.Ready, 1)

	sendReady := func(rd *raft.Ready) {
		if !raft.IsEmptyHardState(rd.HardState) {
			hardState = rd.HardState
		}
		if rd.SoftState != nil {
			atomic.StorePointer(&rc.softState, unsafe.Pointer(rd.SoftState))
			raftState = rd.SoftState.RaftState
		}
		handlingReady = *rd
		advanceC = rc.advanceC
		readyCh <- rd
		readyCh = make(chan *raft.Ready, 1)
	}

	for {

		var propC <-chan *msgWithResult
		var readyC chan (<-chan *raft.Ready)

		if advanceC == nil {
			if node.HasReady() {
				readyC = rc.readyC
			} else {
				propC = rc.propC
			}
		}

		select {
		case <-ticker.C:
			node.Tick()

		case cc := <-rc.confChangeC:
			confChangeCount++
			cc.ID = confChangeCount
			node.ProposeConfChange(cc)

		case msg := <-propC:

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
			//TODO: node.readyWithoutAccept
			rd := node.Ready()

			entry := &rd.Entries[len(rd.Entries)-1]

			if entry.Term != hardState.Term {
				return fmt.Errorf("proposed term(%d) not equal to hardstate.term(%d)???",
					entry.Term, hardState.Term)
			}

			rc.executor.AddContext(entry.Term, entry.Index, msg.resCh)
			msg.idxCh <- entry.Index

			select {
			case rc.readyC <- readyCh:
				sendReady(&rd)
			case <-rc.stopC:
				return nil
			}

		case readyC <- readyCh:
			rd := node.Ready()
			sendReady(&rd)

		case <-advanceC:
			node.Advance(handlingReady)
			handlingReady = raft.Ready{}
			advanceC = nil

		case err := <-rc.transport.ErrorC:
			return err

		case fn := <-rc.funcC:
			fn(node)

		case <-rc.readyDoneC:
			return rc.readyResult

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
	mux.HandleFunc("/raft/snapshot", rc.getSnapshot)
	mux.HandleFunc("/raft/status", rc.getStatus)
	mux.HandleFunc("/raft/campaign", rc.campaign)
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

func getSnapshot(src PeerID) (*pb.Snapshot, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s/raft/snapshot", src.Addr()))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status code = %d, data = %s",
			resp.StatusCode, string(data))
	}

	var snapshot pb.Snapshot
	if err := snapshot.Unmarshal(data); err != nil {
		return nil, err
	}

	return &snapshot, nil
}

func (rc *raftNode) Process(ctx context.Context, m pb.Message) error {
	return rc.withPipeline(ctx, func(node *raft.RawNode) error {
		switch m.Type {
		case pb.MsgHeartbeat:
			if lastWAL, err := rc.storage.LastIndex(); err != nil {
				return err
			} else if m.Commit > lastWAL {
				if rc.fetchingSnapshot {
					m.Commit = 0
					break
				}
				snapshot, err := getSnapshot(PeerID(m.From))
				if err != nil {
					rc.raftLogger.Warning(
						"maybe my log was lost, commit:", m.Commit,
						", from: ", PeerID(m.From),
						", get snapshot failed, err: ", err)
					m.Commit = 0
					break
				}
				rc.fetchingSnapshot = true
				m = pb.Message{
					From:     m.From,
					To:       m.To,
					Term:     m.Term,
					Type:     pb.MsgSnap,
					Snapshot: *snapshot,
				}
				rc.raftLogger.Warning(
					"maybe my log was lost, got snapshot, from: ", PeerID(m.From),
					", term: ", snapshot.Metadata.Term,
					", index: ", snapshot.Metadata.Index,
					", size: ", len(snapshot.Data),
				)
			}
		case pb.MsgHeartbeatResp:
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

func (rc *raftNode) getSnapshot(w http.ResponseWriter, r *http.Request) {
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

func (rc *raftNode) campaign(w http.ResponseWriter, r *http.Request) {
	err := rc.withPipeline(r.Context(), func(node *raft.RawNode) error {
		return node.Campaign()
	})
	fmt.Fprint(w, err)
}
