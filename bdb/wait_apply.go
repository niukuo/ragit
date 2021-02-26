package bdb

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/niukuo/ragit/logging"
)

type waitRequest struct {
	resCh     chan struct{}
	waitIndex uint64
	reqID     uint64
}

type waitApplyRequests struct {
	capacity  int
	mutex     sync.Mutex
	requests  map[uint64]*waitRequest
	waitReqID uint64

	appliedIndex   uint64
	confIndexLeft  uint64
	confIndexRight uint64

	logger logging.Logger
}

func newWaitApplyRequests(cap int, logger logging.Logger) *waitApplyRequests {
	return &waitApplyRequests{
		capacity: cap,
		requests: make(map[uint64]*waitRequest),
		logger:   logger,
	}
}

func (w *waitApplyRequests) addRequest(waitIndex uint64) (*waitRequest, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if waitIndex <= w.appliedIndex {
		return nil, nil
	}

	if len(w.requests) >= w.capacity {
		return nil, fmt.Errorf("pending wait request is more than %d", w.capacity)
	}

	reqID := atomic.AddUint64(&w.waitReqID, 1)
	resCh := make(chan struct{})

	req := &waitRequest{resCh: resCh, waitIndex: waitIndex, reqID: reqID}
	w.requests[reqID] = req
	return req, nil
}

func (w *waitApplyRequests) delRequest(reqID uint64) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	delete(w.requests, reqID)
}

func (w *waitApplyRequests) checkWait_nolock() {
	for id, req := range w.requests {
		if req.waitIndex <= w.appliedIndex {
			close(req.resCh)
			delete(w.requests, id)
		}
	}
}

func (w *waitApplyRequests) applyConfIndex(index uint64) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	defer func() {
		w.logger.Infof("applyConfIndex: %d, appliedIndex: %d, confIndexLeft: %d, confIndexRight: %d",
			index, w.appliedIndex, w.confIndexLeft, w.confIndexRight)
	}()

	if index == 0 {
		w.confIndexLeft = 0
		w.confIndexRight = 0
		return
	}

	if index <= w.confIndexRight {
		return
	}

	if index == w.confIndexRight+1 {
		w.confIndexRight = index
	} else {
		w.confIndexLeft = index
		w.confIndexRight = index
	}

	if w.appliedIndexChase_nolock() {
		w.checkWait_nolock()
	}
}

func (w *waitApplyRequests) applyDataIndex(index uint64) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	defer func() {
		w.logger.Infof("applyDataIndex: %d, appliedIndex: %d, confIndexLeft: %d, confIndexRight: %d",
			index, w.appliedIndex, w.confIndexLeft, w.confIndexRight)
	}()

	if index == 0 {
		w.appliedIndex = 0
		return
	}

	if index <= w.appliedIndex {
		return
	}

	w.appliedIndex = index
	w.appliedIndexChase_nolock()
	w.checkWait_nolock()
}

func (w *waitApplyRequests) appliedIndexChase_nolock() bool { //return if appliedIndex updated
	if w.appliedIndex >= w.confIndexRight {
		w.confIndexLeft = w.appliedIndex
		w.confIndexRight = w.appliedIndex
		return false
	}

	if w.appliedIndex < w.confIndexLeft-1 {
		return false
	}

	w.appliedIndex = w.confIndexRight
	w.confIndexLeft = w.confIndexRight
	return true
}
