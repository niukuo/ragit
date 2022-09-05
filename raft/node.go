package raft

import (
	"context"
	"net/http"

	"github.com/go-git/go-git/v5/plumbing"
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
	InitRouter(mux *http.ServeMux)
	GetStatus(ctx context.Context) (*Status, error)
	ReadIndex(ctx context.Context) (uint64, error)
	BeginTx(initer TxIniter) (*Tx, error)
}
