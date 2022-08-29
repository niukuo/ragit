package raft

import (
	"context"
	"fmt"
	"sort"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/golang/protobuf/proto"
	"github.com/niukuo/ragit/refs"
)

type Tx struct {
	rc Raft

	term     uint64
	unlocker Unlocker

	global bool
	cmds   map[plumbing.ReferenceName]*packp.Command
}

func (t *Tx) Get(refName plumbing.ReferenceName) plumbing.Hash {
	if cmd, ok := t.cmds[refName]; ok {
		return cmd.New
	}
	return plumbing.Hash{}
}

func (t *Tx) Set(refName plumbing.ReferenceName, hash plumbing.Hash) error {

	if cmd, ok := t.cmds[refName]; ok {
		cmd.New = hash
		return nil
	} else if t.global {
		t.cmds[refName] = &packp.Command{
			Name: refName,
			New:  hash,
		}
		return nil
	} else {
		return fmt.Errorf("ref %s not locked", refName)
	}

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
	ctx = WithUnlocker(ctx, t.unlocker)

	req, err := t.rc.Propose(ctx, oplog, handle)
	if err != nil {
		return nil, err
	}
	t.unlocker = nil
	t.cmds = nil
	t.global = false

	return req, nil
}

func (t *Tx) Close() {
	if t.unlocker != nil {
		t.unlocker()
	}
}
