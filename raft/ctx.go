package raft

import (
	"context"
)

type ctxExpectedTerm struct{}

var CtxExpectedTermKey = ctxExpectedTerm{}

func WithExpectedTerm(ctx context.Context, term uint64) context.Context {
	return context.WithValue(ctx, CtxExpectedTermKey, term)
}

type ctxReqUnlocker struct{}

var CtxReqUnlocker = ctxReqUnlocker{}

func WithUnlocker(ctx context.Context, unlocker Unlocker) context.Context {
	return context.WithValue(ctx, CtxReqUnlocker, unlocker)
}
