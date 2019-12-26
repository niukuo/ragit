package raft

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type StateMachine interface {
	OnStart() error
	OnLeaderStart(term uint64)
	OnLeaderStop()
	OnSnapshot(snapshot pb.Snapshot, srcId PeerID) error
	OnApply(ctx context.Context, term, index uint64, oplog refs.Oplog) error
	OnConfState(index uint64, confState pb.ConfState) error
}

type Executor = *executor
type executor struct {
	id        refs.PeerID
	node      *raft.RawNode
	transport *rafthttp.Transport

	sm StateMachine

	// map[index]termAndResult
	doingRequest sync.Map

	leaderTerm uint64

	confIndex uint64
	dataIndex uint64

	confCh  chan confChangeContext
	entryCh chan *pb.Entry
	fnCh    chan func()

	stopCh chan struct{}

	appliedConfIndex uint64
	appliedIndex     uint64

	stopErr error
	stopped chan struct{}

	logger logging.Logger
}

type confChangeContext struct {
	term  uint64
	index uint64
	state pb.ConfState
}

type termAndResult struct {
	term   uint64
	result <-chan applyResult
}

func NewExecutor(
	id refs.PeerID,
	node *raft.RawNode,
	transport *rafthttp.Transport,
	sm StateMachine,
	logger logging.Logger,
) Executor {

	e := &executor{
		id:        id,
		node:      node,
		transport: transport,

		sm: sm,

		confCh:  make(chan confChangeContext),
		entryCh: make(chan *pb.Entry, 100),
		fnCh:    make(chan func()),
		stopCh:  make(chan struct{}),
		stopped: make(chan struct{}),
		logger:  logger,
	}

	return e
}

func (e *executor) Start(
	confIndex uint64,
	dataIndex uint64,
	peers []refs.PeerID,
) error {

	if err := e.sm.OnStart(); err != nil {
		return err
	}

	if err := e.transport.Start(); err != nil {
		return err
	}

	for _, peerid := range peers {
		if peerid != e.id {
			e.transport.AddPeer(types.ID(peerid), []string{"http://" + peerid.Addr()})
		}
	}

	e.confIndex = confIndex
	e.appliedConfIndex = confIndex

	e.dataIndex = dataIndex
	e.appliedIndex = dataIndex

	go e.Run()
	return nil
}

func (e *executor) Stop() error {
	close(e.stopCh)
	<-e.stopped
	return e.stopErr
}

func (e *executor) OnLeaderStart(term uint64) {
	atomic.StoreUint64(&e.leaderTerm, term)
}

func (e *executor) OnLeaderStop() {
	e.withPipeline(func() error {
		if term := atomic.SwapUint64(&e.leaderTerm, 0); term == 0 {
			e.sm.OnLeaderStop()
		}
		return nil
	})
}

func (e *executor) AppendEntry(entry *pb.Entry) error {
	if expect := atomic.LoadUint64(&e.dataIndex) + 1; entry.Index != expect {
		e.logger.Warning("index gap, expect: ", expect,
			", actual: ", entry.Index)
		return fmt.Errorf("index gap, expect %d, actual %d",
			expect, entry.Index)
	}

	var entryCh chan<- *pb.Entry
	var confState pb.ConfState
	var confCh chan<- confChangeContext

	switch typ := entry.Type; typ {
	case pb.EntryNormal:
		entryCh = e.entryCh
	case pb.EntryConfChange:
		if entry.Index > e.confIndex {
			var cc pb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				return err
			}
			confState = *e.node.ApplyConfChange(cc)
			switch typ := cc.Type; typ {
			case pb.ConfChangeAddNode:
				if cc.NodeID != uint64(e.id) {
					e.transport.AddPeer(types.ID(cc.NodeID),
						[]string{"http://" + PeerID(cc.NodeID).Addr()})
				}
			case pb.ConfChangeRemoveNode:
				if cc.NodeID != uint64(e.id) &&
					e.transport.Get(types.ID(cc.NodeID)) != nil {
					e.transport.RemovePeer(types.ID(cc.NodeID))
				}
			default:
				return fmt.Errorf("unsupported conf change type %s", typ)
			}

			confCh = e.confCh
		} else {
			// drop
			confCh = make(chan confChangeContext, 1)
		}
	default:
		return fmt.Errorf("unsupported entry type %s", typ)
	}

	select {
	case entryCh <- entry:
	case confCh <- confChangeContext{
		term:  entry.Term,
		index: entry.Index,
		state: confState,
	}:
		atomic.StoreUint64(&e.confIndex, entry.Index)
	case <-e.stopped:
		return e.stopErr
	}

	atomic.StoreUint64(&e.dataIndex, entry.Index)
	return nil
}

func (e *executor) ApplySnapshot(snap pb.Snapshot, srcId PeerID) error {
	return e.withPipeline(func() error {

		if err := e.sm.OnSnapshot(snap, srcId); err != nil {
			return err
		}

		for len(e.confCh) > 0 {
			<-e.confCh
		}

		for len(e.entryCh) > 0 {
			<-e.entryCh
		}

		e.transport.RemoveAllPeers()
		for _, id := range snap.Metadata.ConfState.Nodes {
			peer := PeerID(id)
			e.transport.AddPeer(types.ID(id),
				[]string{"http://" + peer.Addr()})
		}

		index := snap.Metadata.Index
		atomic.StoreUint64(&e.confIndex, index)
		atomic.StoreUint64(&e.dataIndex, index)
		atomic.StoreUint64(&e.appliedIndex, index)

		return nil
	})
}

func (e *executor) Run() (err error) {

	e.logger.Info("executor started. applied_index: ", e.dataIndex,
		", conf_index: ", e.confIndex)

	defer func() {
		if err != nil {
			e.logger.Warning("executor stopped, err: ", err)
		} else {
			e.logger.Info("executor stopped.")
		}
		e.stopErr = err
		close(e.stopped)
	}()

	for {
		select {
		case <-e.stopCh:
			return nil
		case entry := <-e.entryCh:
			if atomic.CompareAndSwapUint64(&e.leaderTerm, entry.Term, 0) {
				e.sm.OnLeaderStart(entry.Term)
			}
			if err := e.applyEntry(entry); err != nil {
				return err
			}
			atomic.StoreUint64(&e.appliedIndex, entry.Index)
		case conf := <-e.confCh:
			if err := e.sm.OnConfState(
				conf.index, conf.state); err != nil {
				return err
			}
			atomic.StoreUint64(&e.appliedConfIndex, conf.index)
		case fn := <-e.fnCh:
			fn()
		}
	}
}

func (e *executor) applyEntry(entry *pb.Entry) (err error) {
	ctx := context.Background()
	if v, ok := e.doingRequest.Load(entry.Index); ok {
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
	var oplog refs.Oplog
	if err := proto.Unmarshal(entry.Data, &oplog); err != nil {
		return err
	}
	if err := e.sm.OnApply(ctx, entry.Term, entry.Index, oplog); err != nil {
		return err
	}

	return nil
}

func (e *executor) withPipeline(fn func() error) error {
	ch := make(chan error, 1)
	newFn := func() {
		ch <- fn()
		close(ch)
	}

	select {
	case e.fnCh <- newFn:
		return <-ch
	case <-e.stopped:
		if err := e.stopErr; err != nil {
			return err
		}
		return ErrStopped
	}
}

func (e *executor) AddContext(term, index uint64, result <-chan applyResult) {
	e.doingRequest.Store(index, termAndResult{
		term:   term,
		result: result,
	})
}

func (e *executor) DeleteContext(index uint64) {
	e.doingRequest.Delete(index)
}

func (e *executor) Describe(w io.Writer) {
	confIndex := atomic.LoadUint64(&e.confIndex)
	appliedIndex := atomic.LoadUint64(&e.appliedIndex)
	pendingApplyIndex := atomic.LoadUint64(&e.dataIndex)

	type Doing struct {
		index uint64
		term  uint64
	}
	doings := make([]Doing, 0)
	e.doingRequest.Range(func(key interface{}, value interface{}) bool {
		doings = append(doings, Doing{
			index: key.(uint64),
			term:  value.(termAndResult).term,
		})
		return true
	})
	sort.Slice(doings, func(i, j int) bool {
		return doings[i].index < doings[j].index
	})
	fmt.Fprintln(w, "proposing:", len(doings))
	for _, doing := range doings {
		fmt.Fprintf(w, "  term: %d, index: %d\n", doing.term, doing.index)
	}
	fmt.Fprintf(w, "conf_index: %d\n", confIndex)
	fmt.Fprintf(w, "apply_index: (%d, %d]\n", appliedIndex, pendingApplyIndex)
}
