package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
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
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
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

type termAndResult struct {
	term   uint64
	result <-chan applyResult
}

// A key-value stream backed by raft
type Node interface {
	rafthttp.Raft

	getContext(term, index uint64) (*applyResult, bool)
	applyConfChange(cc pb.ConfChange) (*pb.ConfState, error)

	Handler() http.Handler
	InitRouter(mux *http.ServeMux)
	Propose(ctx context.Context, oplog refs.Oplog) error
	GetStatus(ctx context.Context) (*Status, error)
}

type raftNode struct {
	confChangeC chan pb.ConfChange // proposed cluster config changes
	propC       chan *msgWithResult
	funcC       chan func(node *raft.RawNode)

	softState unsafe.Pointer

	// map[index]termAndResult
	doingRequest sync.Map

	readyHandler ReadyHandler
	raftRunner   Runner

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
		confChangeC: make(chan pb.ConfChange),
		propC:       make(chan *msgWithResult),
		funcC:       make(chan func(node *raft.RawNode)),

		raftLogger:  logging.GetLogger("raft"),
		eventLogger: logging.GetLogger("event"),
	}

	rc.raftLogger.Info("starting raft instance, applied_index: ", state.AppliedIndex,
		", committed_index: ", state.HardState.Commit,
		", conf_index: ", state.ConfIndex,
		", conf_state: ", state.ConfState)

	readyHandler, err := StartReadyHandler(
		c.ClusterID,
		id,
		rc,
		c.Storage,
		c.StateMachine,
		state,
	)
	if err != nil {
		return nil, err
	}

	rc.readyHandler = readyHandler

	rc.raftRunner = StartRunner(func(stopC <-chan struct{}) error {
		e := rc.serveRaft(stopC, node, c.TickDuration)
		rc.eventLogger.Warning("node stopped, stopping ready handler. err: ", e)

		rc.readyHandler.Stop()
		<-rc.readyHandler.Done()
		if err := rc.readyHandler.Error(); err != nil {
			e = err
		}

		return e
	})

	return rc, nil
}

func (rc *raftNode) getContext(term, index uint64) (*applyResult, bool) {
	v, ok := rc.doingRequest.Load(index)
	if !ok {
		return nil, false
	}

	tr := v.(termAndResult)
	res, ok := <-tr.result
	if !ok {
		return nil, false
	}

	if tr.term != term {
		res.done <- &errTermChanged{
			proposedTerm:  tr.term,
			committedTerm: term,
		}
		return nil, false
	}

	return &res, true
}

func (msg *msgWithResult) Error(err error) {
	msg.err = err
	close(msg.idxCh)
}

func (rc *raftNode) Stop() error {
	rc.raftRunner.Stop()
	<-rc.raftRunner.Done()
	if err := rc.raftRunner.Error(); err != nil {
		return err
	}
	return nil
}

func (rc *raftNode) serveRaft(stopC <-chan struct{}, node *raft.RawNode, d time.Duration) error {
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
		advanceC = rc.readyHandler.AdvanceC()
		readyCh <- rd
		readyCh = make(chan *raft.Ready, 1)
	}

	for {

		var propC <-chan *msgWithResult
		var readyC chan<- <-chan *raft.Ready

		if advanceC == nil {
			if node.HasReady() {
				readyC = rc.readyHandler.ReadyC()
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

			if err := func() error {
				if raftState != raft.StateLeader {
					return fmt.Errorf("state is %s, cant propose", raftState)
				}

				if msg.expectedTerm != 0 && msg.expectedTerm != hardState.Term {
					return fmt.Errorf("term not match, expected: %d, actual: %d",
						msg.expectedTerm, hardState.Term)
				}

				if err := node.Propose(msg.data); err != nil {
					return err
				}

				return nil
			}(); err != nil {
				msg.Error(err)
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

			rc.doingRequest.Store(entry.Index, termAndResult{
				term:   entry.Term,
				result: msg.resCh,
			})
			msg.idxCh <- entry.Index

			select {
			case rc.readyHandler.ReadyC() <- readyCh:
				sendReady(&rd)

			case <-rc.readyHandler.Done():
				return nil

			case <-stopC:
				return nil
			}

		case readyC <- readyCh:
			rd := node.Ready()
			sendReady(&rd)

		case <-advanceC:
			node.Advance(handlingReady)
			handlingReady = raft.Ready{}
			advanceC = nil

		case fn := <-rc.funcC:
			fn(node)

		case <-rc.readyHandler.Done():
			return nil

		case <-stopC:
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
	rc.readyHandler.InitRouter(mux)
	mux.HandleFunc("/raft/status", rc.getStatus)
	mux.HandleFunc("/raft/campaign", rc.campaign)
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
	case <-rc.raftRunner.Done():
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
	case <-rc.raftRunner.Done():
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
	case <-rc.raftRunner.Done():
		return ErrStopped
	}

	done := make(chan error, 1)

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
	case <-rc.raftRunner.Done():
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
			if lastWAL, err := rc.readyHandler.GetLastIndex(); err != nil {
				return err
			} else if m.Commit > lastWAL {
				if rc.readyHandler.IsFetchingSnapshot() {
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
				rc.readyHandler.SetFetchingSnapshot()
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

func (rc *raftNode) campaign(w http.ResponseWriter, r *http.Request) {
	err := rc.withPipeline(r.Context(), func(node *raft.RawNode) error {
		return node.Campaign()
	})
	fmt.Fprint(w, err)
}

func (rc *raftNode) applyConfChange(cc pb.ConfChange) (*pb.ConfState, error) {
	var state *pb.ConfState
	err := rc.withPipeline(context.Background(), func(node *raft.RawNode) error {
		state = node.ApplyConfChange(cc)
		return nil
	})
	return state, err
}
