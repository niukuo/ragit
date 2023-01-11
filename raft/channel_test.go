package raft

import (
	"errors"
	"testing"
	"time"

	"github.com/niukuo/ragit/logging"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestRefresh(t *testing.T) {
	s := assert.New(t)

	mockStorage := &MockStorage{}
	c := &Channel{
		dialOptions: []grpc.DialOption{
			grpc.WithInsecure(),
		},
		reqTimeout: time.Second,
		storage:    mockStorage,
		conns:      make([]*grpc.ClientConn, 0),

		resetCh: make(chan struct{}, 1),
		logger:  logging.GetLogger("raft.test"),
	}

	s.NoError(c.refresh())

	// no lead
	c.Update(PeerID(0))
	s.NoError(c.refresh())

	idx0 := uint64(0)
	idx100 := uint64(100)
	c.Update(PeerID(123))
	mockStorage.On("LastIndex").Return(idx0, errors.New("bdb error")).Once()
	s.Error(c.refresh())

	mockStorage.On("LastIndex").Return(idx0, nil).Once()
	s.NoError(c.refresh())

	mockStorage.On("LastIndex").Return(idx100, nil).Once()
	mockStorage.On("GetURLsByMemberID", PeerID(123)).Return(
		nil, errors.New("not found")).Once()
	s.Error(c.refresh())

	// partial connected
	mockStorage.On("LastIndex").Return(idx100, nil).Once()
	mockStorage.On("GetURLsByMemberID", PeerID(123)).Return([]string{
		"http://127.0.0.1:1234",
		"http://[::1]:1235",
		"http://foo.com/?foo\nbar",
	}, nil).Once()

	s.NoError(c.refresh())
	s.Len(c.conns, 2)

	s.NoError(c.refresh())

	// reset
	c.ResetConn()
	c.ResetConn()
	s.NoError(c.refresh())
	s.Equal(PeerID(0), c.lead)
	s.Equal(PeerID(0), c.tolead.Load().(PeerID))
	s.Len(c.conns, 0)

	// update before do reset
	c.ResetConn()
	c.Update(PeerID(123))

	mockStorage.On("LastIndex").Return(idx100, nil).Once()
	mockStorage.On("GetURLsByMemberID", PeerID(123)).Return([]string{
		"http://127.0.0.1:1234",
	}, nil).Once()
	s.NoError(c.refresh())
	s.Len(c.conns, 1)
}
