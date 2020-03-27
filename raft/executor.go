package raft

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/golang/protobuf/proto"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type Executor interface {
	Runner

	OnLeaderStart(term uint64)
	OnLeaderStop()

	OnEntry(entry *pb.Entry) error
	OnConfState(index uint64, state pb.ConfState) error

	OnSnapshot(snapsnot pb.Snapshot, srcId PeerID) error

	Describe(w io.Writer)
}

type executor struct {
	sm StateMachine

	node Node

	entryCh chan *pb.Entry
	fnCh    chan func()

	leaderTerm uint64

	dataIndex uint64

	appliedIndex uint64

	Runner

	logger logging.Logger
}

func StartExecutor(
	node Node,
	sm StateMachine,
	dataIndex uint64,
) (Executor, error) {

	if err := sm.OnStart(); err != nil {
		return nil, err
	}

	e := &executor{
		node: node,

		sm: sm,

		entryCh: make(chan *pb.Entry, 100),
		fnCh:    make(chan func()),

		dataIndex:    dataIndex,
		appliedIndex: dataIndex,

		logger: logging.GetLogger("executor"),
	}

	e.Runner = StartRunner(e.Run)

	return e, nil
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

func (e *executor) OnEntry(entry *pb.Entry) error {
	if expect := atomic.LoadUint64(&e.dataIndex) + 1; entry.Index != expect {
		e.logger.Warning("index gap, expect: ", expect,
			", actual: ", entry.Index)
		return fmt.Errorf("index gap, expect %d, actual %d",
			expect, entry.Index)
	}

	select {
	case e.entryCh <- entry:
	case <-e.Runner.Done():
		return ErrStopped
	}

	atomic.StoreUint64(&e.dataIndex, entry.Index)
	return nil
}

func (e *executor) OnConfState(index uint64, state pb.ConfState) error {
	if expect := atomic.LoadUint64(&e.dataIndex) + 1; index != expect {
		e.logger.Warning("index gap, expect: ", expect,
			", actual: ", index)
		return fmt.Errorf("index gap, expect %d, actual %d",
			expect, index)
	}

	if err := e.withPipeline(func() error {
		return e.sm.OnConfState(index, state)
	}); err != nil {
		return err
	}

	atomic.StoreUint64(&e.dataIndex, index)
	return nil
}

func (e *executor) OnSnapshot(snapshot pb.Snapshot, srcId PeerID) error {
	return e.withPipeline(func() error {

		if err := e.sm.OnSnapshot(snapshot, srcId); err != nil {
			return err
		}

		for len(e.entryCh) > 0 {
			<-e.entryCh
		}

		index := snapshot.Metadata.Index
		atomic.StoreUint64(&e.dataIndex, index)
		atomic.StoreUint64(&e.appliedIndex, index)

		return nil
	})
}

func (e *executor) Run(stopC <-chan struct{}) error {

	e.logger.Info("executor started. applied_index: ", e.dataIndex)

	for {
		select {
		case <-stopC:
			return nil
		case entry := <-e.entryCh:
			if atomic.CompareAndSwapUint64(&e.leaderTerm, entry.Term, 0) {
				e.sm.OnLeaderStart(entry.Term)
			}
			if err := e.applyEntry(entry); err != nil {
				return err
			}
			atomic.StoreUint64(&e.appliedIndex, entry.Index)
		case fn := <-e.fnCh:
			fn()
		}
	}
}

func (e *executor) applyEntry(entry *pb.Entry) (err error) {
	ctx := context.Background()
	if res, ok := e.node.getContext(entry.Term, entry.Index); ok {
		ctx = res.context
		defer func() { res.done <- err }()
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
	case <-e.Runner.Done():
		return ErrStopped
	}
}

func (e *executor) Describe(w io.Writer) {
	appliedIndex := atomic.LoadUint64(&e.appliedIndex)
	pendingApplyIndex := atomic.LoadUint64(&e.dataIndex)
	fmt.Fprintf(w, "apply_index: (%d, %d]\n", appliedIndex, pendingApplyIndex)
}
