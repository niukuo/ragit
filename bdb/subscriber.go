package bdb

import (
	"context"
	"sync"

	"github.com/niukuo/ragit/logging"
	ragit "github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
)

type SubManager interface {
	push(term uint64, index uint64, oplog *refs.Oplog)
	stop()

	runSubscriber(ctx context.Context, appliedIndex uint64, ch chan<- ragit.OpEvent)
	pickNext(appliedIndex uint64) (*ragit.OpEvent, <-chan struct{})
}

type subMgr struct {
	cap int

	stopped chan struct{}

	lock sync.Mutex

	fired    chan struct{}
	todelKey uint64
	nextKey  uint64
	// map[appliedIndex]nextEntry
	entries map[uint64]*ragit.OpEvent

	logger logging.Logger
}

var _ SubManager = (*subMgr)(nil)

type SubMgrOption func(*subMgr)

func WithSubMgrLogger(logger logging.Logger) SubMgrOption {
	return func(sm *subMgr) {
		sm.logger = logger
	}
}

func WithSubMgrCap(cap int) SubMgrOption {
	return func(sm *subMgr) {
		sm.cap = cap
	}
}

func newSubMgr(applied uint64, opts ...SubMgrOption) *subMgr {

	s := &subMgr{
		cap:      100,
		fired:    make(chan struct{}),
		todelKey: applied,
		nextKey:  applied,

		stopped: make(chan struct{}),

		logger: logging.GetLogger("ragit.bdb"),
	}

	for _, opt := range opts {
		opt(s)
	}

	s.entries = make(map[uint64]*ragit.OpEvent, s.cap)

	return s

}

func (s *subMgr) stop() {
	close(s.stopped)
	s.lock.Lock()
	defer s.lock.Unlock()
	close(s.fired)
}

func (s *subMgr) push(term, index uint64, oplog *refs.Oplog) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.entries[s.nextKey] = &ragit.OpEvent{
		Term:  term,
		Index: index,
		Ops:   oplog.Ops,
	}
	s.nextKey = index

	for len(s.entries) > s.cap {
		le := s.entries[s.todelKey]
		delete(s.entries, s.todelKey)
		s.todelKey = le.Index
	}

	defer close(s.fired)
	s.fired = make(chan struct{})

}

func (s *subMgr) pickNext(appliedIndex uint64) (*ragit.OpEvent, <-chan struct{}) {
	select {
	case <-s.stopped:
		return nil, nil
	default:
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	if s.todelKey > appliedIndex {
		return nil, nil
	}

	if e, ok := s.entries[appliedIndex]; ok {
		return e, nil
	}

	return nil, s.fired
}

func (s *subMgr) runSubscriber(
	ctx context.Context,
	appliedIndex uint64,
	outCh chan<- ragit.OpEvent,
) {

	s.logger.Info("subscriber started",
		", applied index: ", appliedIndex,
	)

	reason := ""

	defer func() {
		s.logger.Info("subscriber stopped",
			", applied index: ", appliedIndex,
			", reason: ", reason,
		)
	}()

	defer close(outCh)

	for {

		e, waitCh := s.pickNext(appliedIndex)
		if waitCh == nil {
			if e == nil {
				// already dropped
				reason = "truncated"
				return
			}
			select {
			case outCh <- *e:
				appliedIndex = e.Index
				continue
			case <-ctx.Done():
				reason = ctx.Err().Error()
				return
			}
		}

		select {
		case <-waitCh:
		case <-ctx.Done():
			reason = ctx.Err().Error()
			return
		}
	}
}
