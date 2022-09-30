package raft

import (
	"context"
	"net/http"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/niukuo/ragit/refs"
)

type TxIniter func(
	txnLocker MapLocker,
	storage Storage,
) (
	map[plumbing.ReferenceName]plumbing.Hash,
	bool,
	Unlocker,
	error,
)

// A key-value stream backed by raft
type Node interface {
	Handler() http.Handler
	Service() ClusterServer
	InitRouter(mux *http.ServeMux)
	GetStatus(ctx context.Context) (*Status, error)
	ReadIndex(ctx context.Context) (uint64, error)
	Propose(ctx context.Context, cmds []*packp.Command, pack []byte, handle refs.ReqHandle) (DoingRequest, error)
	BeginTx(initer TxIniter) (*Tx, error)
}
