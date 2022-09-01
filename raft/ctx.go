package raft

import (
	"context"
)

type ctxExpectedTerm struct{}

var CtxExpectedTermKey = ctxExpectedTerm{}

func WithExpectedTerm(ctx context.Context, term uint64) context.Context {
	return context.WithValue(ctx, CtxExpectedTermKey, term)
}

type ctxReqDoneCallback struct{}

var CtxReqDoneCallback = ctxReqDoneCallback{}

type ReqDoneCallback func(err error)

func WithReqDoneCallback(ctx context.Context, cb ReqDoneCallback) context.Context {
	return context.WithValue(ctx, CtxReqDoneCallback, cb)
}
