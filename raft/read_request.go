package raft

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/niukuo/ragit/logging"
	"go.etcd.io/etcd/raft"
)

type readRequest struct {
	term  uint64
	id    uint64
	resCh chan uint64
}

type readIndexRequests struct {
	capacity int
	mutex    sync.Mutex
	requests map[uint64]*readRequest
	logger   logging.Logger
}

func newReadIndexRequests(cap int, logger logging.Logger) *readIndexRequests {
	return &readIndexRequests{
		capacity: cap,
		requests: make(map[uint64]*readRequest),
		logger:   logger,
	}
}

func (r *readIndexRequests) addRequest(id uint64) (*readRequest, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if len(r.requests) >= r.capacity {
		return nil, fmt.Errorf("pending read request is more than %d", r.capacity)
	}

	_, ok := r.requests[id]
	if ok {
		return nil, fmt.Errorf("input duplicate id %d, should not happen", id)
	}
	resCh := make(chan uint64, 1) //may timeout before send result, should have buffer
	req := &readRequest{resCh: resCh, id: id}
	r.requests[id] = req
	return req, nil
}

func (r *readIndexRequests) delRequest(id uint64) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.requests, id)
}

func (r *readIndexRequests) check(term uint64) {
	if term <= 0 {
		return
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	for reqID, req := range r.requests {
		if req.term <= 0 {
			continue
		}
		if req.term < term {
			r.logger.Infof("for request %d, term has changed from %d to %d", reqID, req.term, term)
			delete(r.requests, reqID)
			close(req.resCh)
		}
	}
}

func (r *readIndexRequests) setReadStates(readStates []raft.ReadState) {
	if len(readStates) == 0 {
		return
	}

	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, rs := range readStates {
		resId := binary.BigEndian.Uint64(rs.RequestCtx)
		if req, ok := r.requests[resId]; ok {
			req.resCh <- rs.Index
			delete(r.requests, resId)
		} else {
			r.logger.Infof("request %d get readIndex %d, but has been clear", resId, rs.Index)
		}
	}
}
