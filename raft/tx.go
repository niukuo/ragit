package raft

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/niukuo/ragit/refs"
)

type Tx struct {
	rc   Raft
	term uint64

	refCnt   int32
	unlocker Unlocker

	doing int32

	cmds   map[plumbing.ReferenceName]*packp.Command
	insert bool
}

func newTx(
	rc Raft,
	term uint64,
	insert bool,
	unlocker Unlocker,
	cmds map[plumbing.ReferenceName]*packp.Command,
) *Tx {
	return &Tx{
		rc:   rc,
		term: term,

		refCnt:   1,
		unlocker: unlocker,

		cmds:   cmds,
		insert: insert,
	}
}

func (t *Tx) Get(refName plumbing.ReferenceName) *plumbing.Hash {
	if cmd, ok := t.cmds[refName]; ok {
		return &cmd.New
	}
	return nil
}

func (t *Tx) Set(refName plumbing.ReferenceName, hash plumbing.Hash) bool {

	if cmd, ok := t.cmds[refName]; ok {
		cmd.New = hash
		return true
	} else if t.insert {
		cmd = &packp.Command{
			Name: refName,
			New:  hash,
		}
		t.cmds[refName] = cmd
		return true
	} else {
		return false
	}
}

func (t *Tx) Commit(ctx context.Context, pack []byte, handle refs.ReqHandle) (DoingRequest, error) {

	ctx = WithReqDoneCallback(ctx, t.done)
	ctx = WithExpectedTerm(ctx, t.term)

	if old := atomic.SwapInt32(&t.doing, 1); old != 0 {
		return nil, errors.New("already doing")
	}

	if v := atomic.AddInt32(&t.refCnt, 1); v <= 0 {
		panic(v)
	}

	cmds := make([]*packp.Command, 0, len(t.cmds))
	for _, cmd := range t.cmds {
		cmds = append(cmds, cmd)
	}
	req, err := t.rc.Propose(ctx, cmds, pack, handle)
	if err != nil {
		t.done(err)
		return nil, err
	}

	return req, nil
}

func (t *Tx) done(err error) {
	if old := atomic.SwapInt32(&t.doing, 0); old == 0 {
		panic(err)
	}
	if err == nil {
		for _, cmd := range t.cmds {
			cmd.Old = cmd.New
		}
	}
	t.release()
}

func (t *Tx) release() {
	if v := atomic.AddInt32(&t.refCnt, -1); v == 0 {
		if t.unlocker != nil {
			unlocker := t.unlocker
			defer unlocker()
		}
		t.cmds = nil
		t.unlocker = nil
		t.rc = nil
		t.term = 0
	} else if v < 0 {
		panic(v)
	}
}

func (t *Tx) Close() {
	t.release()
}
