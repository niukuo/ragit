package raft

import (
	"context"
	"net/http"

	"github.com/niukuo/ragit/refs"
)

// A key-value stream backed by raft
type Node interface {
	Handler() http.Handler
	InitRouter(mux *http.ServeMux)
	Propose(ctx context.Context, oplog refs.Oplog) error
	GetStatus(ctx context.Context) (*Status, error)
}
