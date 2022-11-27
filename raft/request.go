package raft

import (
	"fmt"
	"io"
	"sync"

	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type DoingRequest interface {
	Done() <-chan struct{}
	Err() error
}

type RequestContextManager = *requestContextManager
type requestContextManager struct {
	lock     sync.Mutex
	requests []*doingRequest

	logger logging.Logger
}

func NewRequestContextManager() RequestContextManager {
	r := &requestContextManager{
		logger: logging.GetLogger("raft.reqctx"),
	}

	return r
}

func (r *requestContextManager) Append(term, index uint64, msg *msgWithResult) error {
	req := &doingRequest{
		index:  index,
		term:   term,
		doneCb: msg.doneCb,
		handle: msg.handle,

		typ: msg.typ,

		done: make(chan struct{}),
	}

	r.lock.Lock()
	defer r.lock.Unlock()
	if l := len(r.requests); l > 0 {
		last := r.requests[l-1]
		if last.index >= index {
			return fmt.Errorf("appending %d/%d to %d/%d", term, index, last.term, last.index)
		}
	}
	r.requests = append(r.requests, req)
	msg.req = req

	return nil
}

func (r *requestContextManager) Take(term, index uint64) (*doingRequest, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if len(r.requests) == 0 {
		return nil, nil
	}

	req := r.requests[0]
	if req.index > index {
		return nil, nil
	}

	if req.index < index {
		err := fmt.Errorf("taking context, index = %d, req.front().index = %d",
			index, req.index)
		return nil, err
	}

	r.requests = r.requests[1:]

	if req.term != term {
		req.fire(&errTermChanged{curTerm: term})
		return nil, nil
	}

	return req, nil
}

func (r *requestContextManager) getFirstAndLast() (int, *doingRequest, *doingRequest) {

	r.lock.Lock()
	defer r.lock.Unlock()
	switch l := len(r.requests); l {
	case 0:
		return 0, nil, nil
	case 1:
		return 1, r.requests[0], nil
	default:
		return l, r.requests[0], r.requests[l-1]
	}

}

func (r *requestContextManager) Clear(err error) {
	r.lock.Lock()
	requests := r.requests
	r.requests = nil
	r.lock.Unlock()

	for _, req := range requests {
		req.fire(err)
	}
}

func (r *requestContextManager) Describe(w io.Writer) {

	l, first, last := r.getFirstAndLast()
	switch l {
	case 0:
		return
	case 1:
		fmt.Fprintf(w, "proposing: %d/%d\n", first.term, first.index)
	default:
		fmt.Fprintf(w, "proposing(%d): %d/%d - %d/%d\n", l,
			first.term, first.index, last.term, last.index)
	}

}

type doingRequest struct {
	index  uint64
	term   uint64
	doneCb ReqDoneCallback
	handle refs.ReqHandle

	typ pb.EntryType

	err  error
	done chan struct{}
}

func (r *doingRequest) fire(err error) {
	r.err = err
	if r.doneCb != nil {
		r.doneCb(err)
	}
	close(r.done)
}

func (r *doingRequest) Done() <-chan struct{} {
	return r.done
}

func (r *doingRequest) Err() error {
	return r.err
}
