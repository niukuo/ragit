package raft

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/go-git/go-git/v5/plumbing"
)

type Key = plumbing.ReferenceName

type MapLocker = *mapLocker
type mapLocker struct {
	items sync.Map

	globalLockErr unsafe.Pointer
	// global locker
	state int32
	ch    unsafe.Pointer
}

func NewMapLocker() MapLocker {
	m := &mapLocker{}
	ch := make(chan struct{})
	atomic.StorePointer(&m.ch, unsafe.Pointer(&ch))
	return m
}

type mapItem struct {
	unlocked <-chan struct{}
}

type Unlocker func()

func (m *mapLocker) LockGlobal(ctx context.Context) error {
	for {

		ch := m.channel()

		if atomic.CompareAndSwapInt32(&m.state, 0, -1) {
			return nil
		}

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *mapLocker) LockGlobalWithError(ctx context.Context, err error) error {
	if !atomic.CompareAndSwapPointer(&m.globalLockErr, nil, unsafe.Pointer(&err)) {
		err = m.globalErr()
		if err == nil {
			err = errors.New("another err have already been set")
		}
		return err
	}

	if err != nil {
		m.broadcast()
	}

	for {
		ch := m.channel()
		if atomic.CompareAndSwapInt32(&m.state, 0, -1) {
			return nil
		}

		select {
		case <-ch:
		case <-ctx.Done():
			if err != nil {
				atomic.StorePointer(&m.globalLockErr, nil)
			}
			return ctx.Err()
		}
	}
}

func (m *mapLocker) UnlockGlobal() {
	atomic.StorePointer(&m.globalLockErr, unsafe.Pointer(nil))
	if ok := atomic.CompareAndSwapInt32(&m.state, -1, 0); !ok {
		panic("Unlock() failed")
	}
	m.broadcast()
}

func (m *mapLocker) globalErr() error {
	p := atomic.LoadPointer(&m.globalLockErr)
	if p == nil {
		return nil
	}
	return (*(*error)(p))
}

func (m *mapLocker) channel() <-chan struct{} {
	return *(*chan struct{})(atomic.LoadPointer(&m.ch))
}

func (m *mapLocker) broadcast() {
	newCh := make(chan struct{})
	ch := *(*chan struct{})(atomic.SwapPointer(&m.ch, unsafe.Pointer(&newCh)))
	close(ch)
}

func (m *mapLocker) RLockGlobal(ctx context.Context) error {
	for {
		if err := m.globalErr(); err != nil {
			return err
		}
		ch := m.channel()
		n := atomic.LoadInt32(&m.state)
		if n >= 0 {
			if atomic.CompareAndSwapInt32(&m.state, n, n+1) {
				return nil
			}
		}

		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

}

func (m *mapLocker) RUnlockGlobal() {
	if n := atomic.AddInt32(&m.state, -1); n < 0 {
		panic("RUnlock() failed")
	} else if n == 0 {
		m.broadcast()
	}

}

func (m *mapLocker) Lock(ctx context.Context, key Key) (Unlocker, error) {

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	done := make(chan struct{})
	item := &mapItem{
		unlocked: done,
	}

	for {
		if err := m.RLockGlobal(ctx); err != nil {
			return nil, err
		}
		v, exist := m.items.LoadOrStore(key, item)
		if !exist {
			release := func() {
				m.items.Delete(key)
				m.RUnlockGlobal()
				close(done)
			}
			return Unlocker(release), nil
		}

		m.RUnlockGlobal()

		oldItem := v.(*mapItem)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-oldItem.unlocked:
		}
	}
}
