package raft

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/niukuo/ragit/logging"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type doingRequest struct {
	index  uint64
	term   uint64
	result <-chan applyResult
}

type RequestContextManager = *requestContextManager
type requestContextManager struct {
	lock     sync.Mutex
	requests []doingRequest

	logger logging.Logger
}

func NewRequestContextManager() RequestContextManager {
	r := &requestContextManager{
		logger: logging.GetLogger("raft.reqctx"),
	}

	return r
}

func (r *requestContextManager) Check(entries []pb.Entry) {
	requests, term := r.tryTruncate(entries)
	if l := len(requests); l > 0 {
		first, last := &requests[0], &requests[l-1]
		r.logger.Warningf("dropping %d contexts from %d/%d to %d/%d", l,
			first.term, first.index, last.term, last.index,
		)
	}
	for i := range requests {
		req := &requests[i]
		res, ok := <-req.result
		if !ok {
			continue
		}
		res.done <- &errTermChanged{
			proposedTerm:  req.term,
			committedTerm: term,
		}
	}
}

func (r *requestContextManager) tryTruncate(entries []pb.Entry) ([]doingRequest, uint64) {

	if len(entries) == 0 {
		return nil, 0
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	if len(r.requests) == 0 {
		return nil, 0
	}

	for i, j := 0, 0; i < len(r.requests) && j < len(entries); {
		req := &r.requests[i]
		entry := &entries[j]
		if req.index < entry.Index {
			i++
			continue
		}
		if req.term < entry.Term {
			drops := r.requests[i:]
			r.requests = r.requests[:i]
			return drops, entry.Term
		}

		j++
		if req.index == entry.Index {
			i++
		}
	}

	return nil, 0
}

func (r *requestContextManager) Append(term, index uint64, resCh <-chan applyResult) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if l := len(r.requests); l > 0 {
		last := &r.requests[l-1]
		if last.index >= index {
			return fmt.Errorf("appending %d/%d to %d/%d", term, index, last.term, last.index)
		}
	}
	r.requests = append(r.requests, doingRequest{
		index:  index,
		term:   term,
		result: resCh,
	})

	return nil
}

func (r *requestContextManager) Take(term, index uint64) (*applyResult, error) {
	req, err := r.takeAndDrop(index)
	if err != nil {
		return nil, err
	}

	if req == nil {
		return nil, nil
	}

	res, ok := <-req.result
	if !ok {
		return nil, nil
	}

	if req.term != term {
		res.done <- &errTermChanged{
			proposedTerm:  req.term,
			committedTerm: term,
		}
		return nil, nil
	}

	return &res, nil
}

func (r *requestContextManager) takeAndDrop(index uint64) (*doingRequest, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if len(r.requests) == 0 {
		return nil, nil
	}

	req := &r.requests[0]
	if req.index > index {
		return nil, nil
	}

	if req.index < index {
		err := fmt.Errorf("taking context, index = %d, req.front().index = %d",
			index, req.index)
		return nil, err
	}

	r.requests = r.requests[1:]
	return req, nil
}

func (r *requestContextManager) getFirstAndLast() (int, *doingRequest, *doingRequest) {

	r.lock.Lock()
	defer r.lock.Unlock()
	switch l := len(r.requests); l {
	case 0:
		return 0, nil, nil
	case 1:
		return 1, &r.requests[0], nil
	default:
		return l, &r.requests[0], &r.requests[l-1]
	}

}

func (r *requestContextManager) Clear(err error) {
	requests := r.clear()
	for _, req := range requests {
		res, ok := <-req.result
		if !ok {
			continue
		}
		res.done <- err
	}
}

func (r *requestContextManager) clear() []doingRequest {
	r.lock.Lock()
	defer r.lock.Unlock()

	requests := r.requests
	r.requests = []doingRequest{}

	return requests
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

type AsyncHandle = *asyncHandle
type asyncHandle struct {
	index uint64
	resCh chan<- applyResult
}

func (h *asyncHandle) Ignore() {
	close(h.resCh)
}

func (h *asyncHandle) Wait(ctx context.Context) error {

	defer h.Ignore()

	done := make(chan error, 1)

	select {
	case h.resCh <- applyResult{
		context: ctx,
		done:    done,
	}:
		return <-done
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}
