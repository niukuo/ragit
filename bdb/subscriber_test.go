package bdb

import (
	"context"
	"fmt"
	"testing"
	"time"

	ragit "github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/suite"
)

func TestSubManager(t *testing.T) {

	suite.Run(t, &SubManagerSuite{})

}

type SubManagerSuite struct {
	suite.Suite
}

func (s *subMgr) startListener(
	ctx context.Context,
	appliedIndex uint64,
) <-chan ragit.OpEvent {

	outCh := make(chan ragit.OpEvent)
	go s.runSubscriber(ctx, appliedIndex, outCh)
	return outCh

}

func (s *SubManagerSuite) TestPush() {

	ptr := newSubMgr(10, WithSubMgrCap(1))
	var mgr SubManager = ptr

	e, ch := mgr.pickNext(9)
	s.Nil(e)
	s.Nil(ch)

	e, ch = mgr.pickNext(10)
	s.Nil(e)
	s.NotNil(ch)

	select {
	case <-ch:
		s.Fail("channel shouldnt fire")
	default:
	}

	s.Len(ptr.entries, 0)
	s.Equal(uint64(10), ptr.todelKey)
	s.Equal(uint64(10), ptr.nextKey)

	outCh := ptr.startListener(context.TODO(), 9)
	select {
	case _, ok := <-outCh:
		s.False(ok)
	case <-time.After(1 * time.Second):
		s.Fail("listener should be stopped")
	}

	ctx, cancel := context.WithCancel(context.Background())
	outCh = ptr.startListener(ctx, 10)
	select {
	case <-outCh:
		s.Fail("listener shouldnt be fired")
	default:
	}

	mgr.push(1, 13, &refs.Oplog{})

	s.Len(ptr.entries, 1)
	s.Equal(uint64(10), ptr.todelKey)
	s.Equal(uint64(13), ptr.nextKey)

	select {
	case <-ch:
	default:
		s.Fail("channel should fire, but not")
	}

	// listener is waiting for fire
	cancel()
	timeoutC := time.After(1 * time.Second)
	select {
	case _, ok := <-outCh:
		if ok {
			s.T().Log("fired before closed")
			select {
			case _, ok = <-outCh:
				s.False(ok)
			case <-timeoutC:
				s.Fail("wait timeout")
			}
		}
	case <-timeoutC:
		s.Fail("wait timeout")
	}

	ctx, cancel = context.WithCancel(context.Background())
	fmt.Printf("start: %#v\n", ptr.entries)
	outCh = ptr.startListener(ctx, 10)
	select {
	case _, ok := <-outCh:
		s.True(ok)
	case <-time.After(1 * time.Second):
		s.Fail("wait timeout")
	}

	// listener is waiting for next push
	select {
	case <-outCh:
		s.Fail("listener shouldnt be fired")
	default:
	}
	cancel()
	select {
	case _, ok := <-outCh:
		s.False(ok)
	case <-time.After(1 * time.Second):
		s.Fail("listener should be stopped")
	}

	e, ch = mgr.pickNext(10)
	s.NotNil(e)
	s.Nil(ch)
	s.Equal(uint64(13), e.Index)

	mgr.push(1, 15, &refs.Oplog{})
	s.Len(ptr.entries, 1)
	s.Equal(uint64(13), ptr.todelKey)
	s.Equal(uint64(15), ptr.nextKey)

	e, ch = mgr.pickNext(10)
	s.Nil(e)
	s.Nil(ch)

	mgr.push(1, 15, &refs.Oplog{})
}

func (s *SubManagerSuite) TestStop() {

	ptr := newSubMgr(10)
	var mgr SubManager = ptr

	e, ch := mgr.pickNext(10)
	s.Nil(e)
	s.NotNil(ch)

	select {
	case <-ch:
		s.Fail("channel shouldnt fire")
	default:
	}

	mgr.stop()

	select {
	case <-ch:
	default:
		s.Fail("channel should fire, but not")
	}

	e, ch = mgr.pickNext(10)

	s.Nil(e)
	s.Nil(ch)

}
