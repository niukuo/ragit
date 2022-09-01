package raft

import (
	"context"
	"net/http"

	"github.com/go-git/go-git/v5/plumbing"
)

// A key-value stream backed by raft
type Node interface {
	Handler() http.Handler
	InitRouter(mux *http.ServeMux)
	GetStatus(ctx context.Context) (*Status, error)
	ReadIndex(ctx context.Context) (uint64, error)
	BeginTx(ctx context.Context,
		refName plumbing.ReferenceName,
		refNames ...plumbing.ReferenceName,
	) (*Tx, error)
	BeginGlobalTx(ctx context.Context,
		lockErr error,
	) (*Tx, error)
}
