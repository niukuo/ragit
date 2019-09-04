package raft

import (
	"context"
	"io"
)

type ctxExpectedTerm struct{}

var CtxExpectedTermKey = ctxExpectedTerm{}

func WithExpectedTerm(ctx context.Context, term uint64) context.Context {
	return context.WithValue(ctx, CtxExpectedTermKey, term)
}

type ctxWriterKeyType struct{}

var CtxWriterKey = ctxWriterKeyType{}

func WithResponseWriter(ctx context.Context, w io.Writer) context.Context {
	return context.WithValue(ctx, CtxWriterKey, w)
}
