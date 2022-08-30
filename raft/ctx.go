package raft

import (
	"context"
)

type ctxExpectedTerm struct{}

var CtxExpectedTermKey = ctxExpectedTerm{}

func WithExpectedTerm(ctx context.Context, term uint64) context.Context {
	return context.WithValue(ctx, CtxExpectedTermKey, term)
}
