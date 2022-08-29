package raft_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/niukuo/ragit/raft"
	"github.com/stretchr/testify/assert"
)

func TestLock(t *testing.T) {

	s := assert.New(t)

	locker := raft.NewMapLocker()
	ch := make(chan error)
	lock := func(ctx context.Context, key plumbing.ReferenceName, cancel *raft.Unlocker) {
		unlock, err := locker.Lock(ctx, key)
		s.Equal(cancel == nil, unlock == nil)
		if cancel != nil {
			*cancel = unlock
		}
		ch <- err
	}

	var uk1 raft.Unlocker
	go lock(context.Background(), "key1", &uk1)
	s.NoError(<-ch)

	var uk2 raft.Unlocker
	go lock(context.Background(), "key2", &uk2)
	s.NoError(<-ch)

	ctx, cancel := context.WithTimeout(context.Background(),
		1*time.Millisecond)
	go lock(ctx, "key1", nil)
	s.Equal(context.DeadlineExceeded, <-ch)
	cancel()

	go lock(context.Background(), "key1", &uk1)
	select {
	case err := <-ch:
		s.FailNow("shouldnt return", err)
	case <-time.After(500 * time.Millisecond):
	}
	uk1()
	s.NoError(<-ch)
	cancel()
}
