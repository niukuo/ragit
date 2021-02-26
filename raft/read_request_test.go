package raft

import (
	"encoding/binary"
	"testing"

	"github.com/niukuo/ragit/logging"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft"
)

func TestReadIndexRequests(t *testing.T) {
	s := assert.New(t)

	requests := newReadIndexRequests(10, logging.GetLogger("test"))

	requests.delRequest(100)
	requests.check(1)

	reqOld, err := requests.addRequest(1)
	s.NoError(err)
	s.EqualValues(reqOld.id, 1)
	reqOld.term = 5

	reqNew, err := requests.addRequest(2)
	s.EqualValues(reqNew.id, 2)
	s.NoError(err)
	reqNew.term = 10

	s.Len(requests.requests, 2)
	requests.check(8)
	s.Len(requests.requests, 1)
	_, ok := <-reqOld.resCh
	s.False(ok)

	testIndex := uint64(12)
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(2))
	rs := []raft.ReadState{{Index: testIndex, RequestCtx: data}}

	requests.setReadStates(rs)
	s.Len(requests.requests, 0)
	res, ok := <-reqNew.resCh
	s.True(ok)
	s.Equal(res, testIndex)
}
