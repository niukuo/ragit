package raft

import (
	"errors"
	"sync/atomic"

	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var errSkipEntry = errors.New("skip entry")

func ErrSkipEntry() error {
	return errSkipEntry
}

type Executor interface {
	Runner

	OnLeaderStart(term uint64)
	OnLeaderStop()

	OnEntry(entry *pb.Entry) error
	OnConfState(index uint64, state pb.ConfState, members []refs.Member, opType pb.ConfChangeType) error

	OnSnapshot(snapsnot pb.Snapshot, srcId PeerID) error
}

type executor struct {
	sm StateMachine

	raft Raft

	entryCh chan *pb.Entry
	fnCh    chan func()

	leaderTerm uint64

	Runner

	logger logging.Logger
}

func StartExecutor(
	raft Raft,
	sm StateMachine,
) (Executor, error) {

	if err := sm.OnStart(); err != nil {
		return nil, err
	}

	e := &executor{
		raft: raft,

		sm: sm,

		entryCh: make(chan *pb.Entry, 100),
		fnCh:    make(chan func()),

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
	select {
	case e.entryCh <- entry:
		executorEntryQueueSize.Inc()
	case <-e.Runner.Done():
		return ErrStopped
	}

	return nil
}

func (e *executor) OnConfState(index uint64, state pb.ConfState, members []refs.Member, opType pb.ConfChangeType) error {
	if err := e.withPipeline(func() error {
		return e.sm.OnConfState(index, state, members, opType)
	}); err != nil {
		return err
	}

	return nil
}

func (e *executor) OnSnapshot(snapshot pb.Snapshot, srcId PeerID) error {
	return e.withPipeline(func() error {

		if err := e.sm.OnSnapshot(snapshot, srcId); err != nil {
			return err
		}

		for len(e.entryCh) > 0 {
			<-e.entryCh
			executorEntryQueueSize.Dec()
		}

		return nil
	})
}

func (e *executor) Run(stopC <-chan struct{}) error {

	for {
		select {
		case <-stopC:
			return nil
		case entry := <-e.entryCh:
			executorEntryQueueSize.Dec()
			// A new raft leader first tries to commit a no-op log entry
			// to implicit commit previous-term logs and figure out current commitIndex.
			if atomic.CompareAndSwapUint64(&e.leaderTerm, entry.Term, 0) {
				e.sm.OnLeaderStart(entry.Term)
			}
			if err := e.applyEntry(entry); err != nil {
				return err
			}
		case fn := <-e.fnCh:
			fn()
		}
	}
}

func (e *executor) applyEntry(entry *pb.Entry) (err error) {
	req, err := e.raft.getContext(entry.Term, entry.Index)
	if err != nil {
		return err
	}

	var handle refs.ReqHandle

	if req != nil {
		handle = req.handle
		defer func() {
			if err != nil {
				req.fire(err)
			} else {
				var respErr error
				// NOTE: pb.EntryConfChange can be dropped to no-op pb.EntryNormal in etcd/raft
				// if there is prior unapplied configuration change in its log.
				if len(entry.Data) == 0 && req.typ == pb.EntryConfChange {
					respErr = errors.New("dropped conf change")
				}

				req.fire(respErr)
			}
		}()
	}

	var oplog refs.Oplog
	if err = proto.Unmarshal(entry.Data, &oplog); err != nil {
		return err
	}
	if err = e.sm.OnApply(entry.Term, entry.Index, &oplog, handle); err != nil {
		if handle != nil || !errors.Is(err, errSkipEntry) {
			return err
		}
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
