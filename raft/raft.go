package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

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

type termAndResult struct {
	term   uint64
	result <-chan applyResult
}

type msgWithResult struct {
	expectedTerm uint64
	data         []byte
	idxCh        chan<- uint64
	err          error
	resCh        <-chan applyResult
}

type ApplyResult = applyResult
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
	storage Storage

	transport  *rafthttp.Transport
	stopC      chan struct{} // signals proposal channel closed
	raftResult error
	raftDoneC  chan struct{}

	raftLogger  logging.Logger
	eventLogger logging.Logger
}

// newRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func RunNode(c Config, peers []PeerID, storage Storage) (Node, error) {

	rpeers := make([]raft.Peer, 0, len(peers))
	for _, peer := range peers {
		rpeers = append(rpeers, raft.Peer{ID: uint64(peer)})
	}

	id := PeerID(c.ID)
	if c.Storage == nil {
		c.Storage = storage
	}

	node, err := raft.NewRawNode(&c.Config, rpeers)
	if err != nil {
		return nil, err
	}

	rc := &raftNode{
		id:          id,
		confChangeC: make(chan pb.ConfChange),
		propC:       make(chan *msgWithResult),
		funcC:       make(chan func(node *raft.RawNode)),

		storage: storage,

		stopC:     make(chan struct{}),
		raftDoneC: make(chan struct{}),

		raftLogger:  logging.GetLogger("raft"),
		eventLogger: logging.GetLogger("event"),
	}

	lastWAL, err := storage.LastIndex()
	if err != nil {
		return nil, err
	}
	rc.lastWAL = lastWAL

	transport := &rafthttp.Transport{
		Logger:      zap.NewNop(),
		ID:          types.ID(id),
		ClusterID:   c.ClusterID,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(id.String()),
		ErrorC:      make(chan error),
	}

	if err := transport.Start(); err != nil {
		return nil, err
	}

	for _, peerid := range peers {
		if peerid != id {
			transport.AddPeer(types.ID(peerid), []string{"http://" + peerid.Addr()})
		}
	}

	rc.transport = transport

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
func (rc *raftNode) applyEntries(node *raft.RawNode, srcId PeerID, ents []pb.Entry) error {
	sm := rc.storage
	for i := range ents {
		if err := rc.applyEntry(sm, node, srcId, &ents[i]); err != nil {
			return err
		}
	}
	return nil
}

func (rc *raftNode) applyEntry(sm Storage, node *raft.RawNode,
	srcId PeerID, entry *pb.Entry) (err error) {
	ctx := context.Background()
	if v, ok := rc.doingRequest.Load(entry.Index); ok {
		tr := v.(termAndResult)
		if res, ok := <-tr.result; ok {
			if tr.term != entry.Term {
				res.done <- &errTermChanged{
					proposedTerm:  tr.term,
					committedTerm: entry.Term,
				}
			} else {
				ctx = res.context
				defer func() {
					res.done <- err
				}()
			}
		}
	}

	switch entry.Type {
	case pb.EntryNormal:
		var oplog refs.Oplog
		if err := proto.Unmarshal(entry.Data, &oplog); err != nil {
			return err
		}
		if err := sm.Apply(ctx, entry.Term, entry.Index, oplog, srcId); err != nil {
			return err
		}

	case pb.EntryConfChange:
		var cc pb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			return err
		}
		if err := sm.UpdateConfState(entry.Term, entry.Index,
			*node.ApplyConfChange(cc)); err != nil {
			return err
		}
		switch cc.Type {
		case pb.ConfChangeAddNode:
			if cc.NodeID != uint64(rc.id) {
				rc.transport.AddPeer(types.ID(cc.NodeID), []string{"http://" + PeerID(cc.NodeID).Addr()})
			}
		case pb.ConfChangeRemoveNode:
			if cc.NodeID != uint64(rc.id) && rc.transport.Get(types.ID(cc.NodeID)) != nil {
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	return nil
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) Stop() error {
	close(rc.stopC)
	rc.transport.Stop()
	<-rc.raftDoneC
	return rc.raftResult
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
			rd.Snapshot.Metadata,
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
				from := raftState
				to := rd.SoftState.RaftState
				rc.eventLogger.Infof("soft state changed from %s to %s, term: %d",
					raftState, rd.SoftState.RaftState, hardState.Term)
				raftState = rd.SoftState.RaftState
				leader = PeerID(rd.SoftState.Lead)
				if from != raft.StateLeader && to == raft.StateLeader {
					rc.storage.OnLeaderStart(hardState.Term)
				} else if from == raft.StateLeader && to != raft.StateLeader {
					rc.storage.OnLeaderStop()
				}
			}

			objSrcName := leader
			if raftState == raft.StateLeader {
				objSrcName = PeerID(0)
			}

			if err := rc.storage.Save(rd.HardState, rd.Entries, rd.Snapshot, objSrcName, rd.MustSync); err != nil {
				return err
			}
			if len(rd.Entries) > 0 {
				rc.lastWAL = rd.Entries[len(rd.Entries)-1].Index
			}
			rc.transport.Send(rd.Messages)
			if err := rc.applyEntries(node, objSrcName, rd.CommittedEntries); err != nil {
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

			rc.doingRequest.Store(entry.Index, termAndResult{
				term:   entry.Term,
				result: msg.resCh,
			})

			if entry.Term != hardState.Term {
				return fmt.Errorf("proposed term(%d) not equal to hardstate.term(%d)???",
					entry.Term, hardState.Term)
			}

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
	mux.HandleFunc("/raft/status", rc.getStatus)
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

func (rc *raftNode) getStatus(w http.ResponseWriter, r *http.Request) {

	status, err := rc.GetStatus(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")

	encoder.Encode(status)
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
		defer rc.doingRequest.Delete(index)
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

func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
