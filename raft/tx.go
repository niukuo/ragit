package raft

import (
	"context"
	"errors"
	"sort"
	"sync/atomic"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/golang/protobuf/proto"
	"github.com/niukuo/ragit/refs"
)

type Tx struct {
	rc     Raft
	term   uint64
	global bool

	refCnt   int32
	unlocker Unlocker

	doing int32

	cmds map[plumbing.ReferenceName]*packp.Command
}

func newTx(
	rc Raft,
	term uint64,
	global bool,
	unlocker Unlocker,
	cmds map[plumbing.ReferenceName]*packp.Command,
) *Tx {
	return &Tx{
		rc:     rc,
		term:   term,
		global: global,

		refCnt:   1,
		unlocker: unlocker,

		cmds: cmds,
	}
}

func (t *Tx) Get(refName plumbing.ReferenceName) *plumbing.Hash {
	if cmd, ok := t.cmds[refName]; ok {
		return &cmd.New
	} else if t.global {
		cmd := &packp.Command{
			Name: refName,
		}
		t.cmds[refName] = cmd
		return &cmd.New
	}
	return nil
}

func (t *Tx) Commit(ctx context.Context, pack []byte, handle refs.ReqHandle) (DoingRequest, error) {

	ops := make([]*refs.Oplog_Op, 0, len(t.cmds))

	for _, cmd := range t.cmds {

		var fromHash, toHash []byte
		if !cmd.Old.IsZero() {
			hash := cmd.Old
			fromHash = hash[:]
		}

		if !cmd.New.IsZero() {
			hash := cmd.New
			toHash = hash[:]
		}

		ops = append(ops, &refs.Oplog_Op{
			Name:      proto.String(string(cmd.Name)),
			OldTarget: fromHash,
			Target:    toHash,
		})

	}

	sort.Slice(ops, func(i, j int) bool {
		return *ops[i].Name < *ops[j].Name
	})

	oplog := refs.Oplog{
		Ops:     ops,
		ObjPack: pack,
	}

	ctx = WithExpectedTerm(ctx, t.term)
	ctx = WithReqDoneCallback(ctx, t.done)

	if old := atomic.SwapInt32(&t.doing, 1); old != 0 {
		return nil, errors.New("already doing")
	}
	if v := atomic.AddInt32(&t.refCnt, 1); v <= 0 {
		panic(v)
	}
	req, err := t.rc.Propose(ctx, oplog, handle)
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
		unlocker := t.unlocker
		defer unlocker()
		t.cmds = nil
		t.unlocker = nil
		t.rc = nil
		t.term = 0
		t.global = false
	} else if v < 0 {
		panic(v)
	}
}

func (t *Tx) Close() {
	t.release()
}
