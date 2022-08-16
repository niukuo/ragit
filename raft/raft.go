package raft

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

var ErrStopped = errors.New("stopped")

type msgWithResult struct {
	expectedTerm uint64
	data         []byte
	resCh        <-chan applyResult

	err   error
	idxCh chan<- uint64
}

type applyResult struct {
	context context.Context
	done    chan<- error
}

type Raft interface {
	rafthttp.Raft

	Runner
	Start(node *raft.RawNode, readyHandler ReadyHandler, d time.Duration)

	InitRouter(mux *http.ServeMux)

	Propose(ctx context.Context, oplog refs.Oplog) (AsyncHandle, error)

	getContext(term, index uint64) (*applyResult, error)
	applyConfChange(confState pb.ConfChange) (*pb.ConfState, error)

	withPipeline(ctx context.Context, fn func(node *raft.RawNode) error) error

	Describe(w io.Writer)

	getReadIndex(ctx context.Context, id uint64) (uint64, error)
	setReadStates([]raft.ReadState)
}

type raftNode struct {
	confChangeC chan pb.ConfChange // proposed cluster config changes
	propC       chan *msgWithResult
	funcC       chan func(node *raft.RawNode)
	readIndexC  chan *readRequest

	softState unsafe.Pointer

	requests     RequestContextManager
	readRequests *readIndexRequests

	readyHandler ReadyHandler

	newMemberID func(addr []string) refs.PeerID

	Runner

	raftLogger  logging.Logger
	eventLogger logging.Logger
}

type ConfChangeParams struct {
	PeerUrls []string `json:"peerUrls"`
}

func NewRaft(config Config) Raft {
	raftLogger := logging.GetLogger("raft")
	eventLogger := logging.GetLogger("event")
	rc := &raftNode{
		confChangeC: make(chan pb.ConfChange),
		propC:       make(chan *msgWithResult),
		funcC:       make(chan func(node *raft.RawNode)),
		readIndexC:  make(chan *readRequest),

		requests:     NewRequestContextManager(),
		readRequests: newReadIndexRequests(100, raftLogger),

		newMemberID: config.NewMemberID,

		raftLogger:  raftLogger,
		eventLogger: eventLogger,
	}

	return rc
}

func (rc *raftNode) Start(
	node *raft.RawNode,
	readyHandler ReadyHandler,
	d time.Duration,
) {

	rc.readyHandler = readyHandler
	startChan := make(chan struct{})
	rc.Runner = StartRunner(func(stopC <-chan struct{}) error {
		<-startChan
		err := rc.serveRaft(stopC, node, d)
		rc.requests.Clear(ErrStopped)
		return err
	})
	close(startChan)
}

func (rc *raftNode) getContext(term, index uint64) (*applyResult, error) {
	return rc.requests.Take(term, index)
}

func (msg *msgWithResult) Error(err error) {
	msg.err = err
	close(msg.idxCh)
}

func (rc *raftNode) serveRaft(stopC <-chan struct{}, node *raft.RawNode, d time.Duration) error {
	ticker := time.NewTicker(d)
	defer ticker.Stop()

	var term uint64
	var nextIndex uint64

	var rd raft.Ready
	var advanceC <-chan struct{}
	readyCh := make(chan *raft.Ready, 1)

	for {

		var readyC chan<- <-chan *raft.Ready

		if advanceC == nil && node.HasReady() {
			readyC = rc.readyHandler.ReadyC()
		}

		select {
		case <-ticker.C:
			node.Tick()

		case cc := <-rc.confChangeC:
			if nextIndex == 0 {
				break
			}
			cc.ID = nextIndex
			if err := node.ProposeConfChange(cc); err == nil {
				nextIndex++
			}
		case msg := <-rc.propC:

			if err := func() error {
				if nextIndex == 0 {
					return errors.New("not leader, cant propose")
				}

				if msg.expectedTerm != 0 && msg.expectedTerm != term {
					return fmt.Errorf("term not match, expected: %d, actual: %d",
						msg.expectedTerm, term)
				}

				if err := node.Propose(msg.data); err != nil {
					return err
				}

				index := nextIndex
				nextIndex++

				if err := rc.requests.Append(term, index, msg.resCh); err != nil {
					return err
				}
				msg.idxCh <- index

				return nil
			}(); err != nil {
				msg.Error(err)
				break
			}
		case req := <-rc.readIndexC:
			if err := func() error {
				req.term = term
				ctxToSend := make([]byte, 8)
				binary.BigEndian.PutUint64(ctxToSend, req.id)
				node.ReadIndex(ctxToSend)
				return nil
			}(); err != nil {
				rc.raftLogger.Errorf("deal read index %d err, %s", req.id, err.Error())
			}

		case readyC <- readyCh:
			rd = node.Ready()

			if !raft.IsEmptyHardState(rd.HardState) {
				term = rd.HardState.Term
				rc.readRequests.check(term)
			}

			if rd.SoftState != nil {
				if rd.SoftState.RaftState == raft.StateLeader {
					index := rd.Entries[len(rd.Entries)-1].Index + 1
					if nextIndex != 0 && nextIndex != index {
						err := fmt.Errorf("next_index mismatch, expected: %d, actual: %d",
							nextIndex, index)
						rc.eventLogger.Error(err)
						return err
					}
					rc.eventLogger.Infof("become leader at term %d, next_index: %d",
						term, index)
					nextIndex = index
				} else if nextIndex != 0 {
					rc.eventLogger.Warningf("lost leader, next_index: %d", nextIndex)
					nextIndex = 0
				} else {
					rc.eventLogger.Infof("state: %s, leader: %s",
						rd.SoftState.RaftState, PeerID(rd.SoftState.Lead))
				}
				atomic.StorePointer(&rc.softState, unsafe.Pointer(rd.SoftState))
			}

			if l := len(rd.Entries); nextIndex != 0 && l != 0 {
				if actual := rd.Entries[l-1].Index + 1; nextIndex != actual {
					err := fmt.Errorf("next_index check failed, expected: %d, actual: %d",
						nextIndex, actual)
					rc.eventLogger.Error(err)
					return err
				}
			}

			advanceC = rc.readyHandler.AdvanceC()
			readyCh <- &rd
			readyCh = make(chan *raft.Ready, 1)

			rc.requests.Check(rd.Entries)

		case <-advanceC:
			node.Advance(rd)
			rd = raft.Ready{}
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

func (rc *raftNode) InitRouter(mux *http.ServeMux) {
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
	case <-rc.Runner.Done():
		return ErrStopped
	}
}

func (rc *raftNode) getMemberStatus(w http.ResponseWriter, r *http.Request) {
	var memberStatus *MemberStatus

	if err := rc.withPipeline(r.Context(), func(node *raft.RawNode) error {
		var errStatus error
		memberStatus, errStatus = (Status)(node.Status()).MemberStatus(rc.readyHandler.GetMemberAddrs)
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

func (rc *raftNode) confChange(w http.ResponseWriter, r *http.Request) {
	var opType pb.ConfChangeType
	var memberID refs.PeerID
	var peerUrls []string

	switch r.Method {
	case http.MethodPost:
		opType = pb.ConfChangeAddNode
		if r.URL.Query().Get("mode") == "learner" {
			opType = pb.ConfChangeAddLearnerNode
		}

		var ccParams ConfChangeParams
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		err := dec.Decode(&ccParams)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(ccParams.PeerUrls) == 0 {
			http.Error(w, "peer urls is empty", http.StatusBadRequest)
			return
		}
		memberID = rc.newMemberID(ccParams.PeerUrls)
		peerUrls = ccParams.PeerUrls
	case http.MethodDelete:
		opType = pb.ConfChangeRemoveNode
		id := strings.TrimPrefix(r.URL.Path, "/raft/members/")
		val, err := strconv.ParseUint(id, 16, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		memberID = refs.PeerID(val)
	default:
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	cc := pb.ConfChange{
		Type:   opType,
		NodeID: uint64(memberID),
	}

	if opType != pb.ConfChangeRemoveNode {
		member := refs.NewMember(memberID, peerUrls)
		mb, err := json.Marshal(member)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		cc.Context = mb
	}

	rc.confChangeC <- cc

	rc.eventLogger.Infof("confChange, op %s, node %s, addrs %+v", opType.String(), memberID, peerUrls)
	// As above, optimistic that raft will apply the conf change
	w.WriteHeader(http.StatusNoContent)
}

func (rc *raftNode) Propose(ctx context.Context, oplog refs.Oplog) (AsyncHandle, error) {
	if err := refs.Validate(oplog); err != nil {
		return nil, err
	}

	content, err := proto.Marshal(&oplog)
	if err != nil {
		return nil, err
	}

	idxCh := make(chan uint64, 1)
	resCh := make(chan applyResult) // must no buffer

	msg := msgWithResult{
		data:  content,
		resCh: resCh,

		idxCh: idxCh,
	}

	if v := ctx.Value(CtxExpectedTermKey); v != nil {
		msg.expectedTerm = v.(uint64)
	}

	select {
	case rc.propC <- &msg:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-rc.Runner.Done():
		return nil, ErrStopped
	}

	handle := asyncHandle{
		resCh: resCh,
	}

	if i, ok := <-idxCh; !ok {
		err := msg.err
		if err == nil {
			err = errors.New("propose failed")
		}
		return nil, err
	} else {
		handle.index = i
	}

	rc.eventLogger.Info("proposed, index: ", handle.index, ", opcnt: ", len(oplog.Ops),
		", expectedTerm: ", msg.expectedTerm)

	return &handle, nil
}

func (rc *raftNode) Process(ctx context.Context, m pb.Message) error {
	return rc.withPipeline(ctx, func(node *raft.RawNode) error {
		switch m.Type {
		case pb.MsgHeartbeat:
		case pb.MsgHeartbeatResp, pb.MsgReadIndex, pb.MsgReadIndexResp:
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

func (rc *raftNode) getReadIndex(ctx context.Context, id uint64) (uint64, error) {
	req, err := rc.readRequests.addRequest(id)
	if err != nil {
		return 0, err
	}
	defer rc.readRequests.delRequest(id)

	select {
	case rc.readIndexC <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-rc.Runner.Done():
		return 0, ErrStopped
	}

	select {
	case res, ok := <-req.resCh:
		if ok {
			return res, nil
		}
		return 0, errors.New("read index failed, term has changed")
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-rc.Runner.Done():
		return 0, ErrStopped
	}
}

func (rc *raftNode) setReadStates(readStates []raft.ReadState) {
	rc.readRequests.setReadStates(readStates)
}
