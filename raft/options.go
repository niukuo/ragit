package raft

import (
	"time"

	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"google.golang.org/grpc"
)

type NodeOptions interface {
	applyRaft(r *raftNode)
	applyReadyHandler(r *readyHandler)
}

type raftOptions func(r *raftNode)

func (o raftOptions) applyRaft(r *raftNode) {
	o(r)
}
func (o raftOptions) applyReadyHandler(r *readyHandler) {}

type readyOptions func(r *readyHandler)

func (o readyOptions) applyRaft(r *raftNode) {}
func (o readyOptions) applyReadyHandler(r *readyHandler) {
	o(r)
}

func WithNewMemberID(fn func(peerURLs []string) refs.PeerID) NodeOptions {
	return readyOptions(func(r *readyHandler) {
		r.newMemberID = fn
	})
}

func WithTxnLocker(txnLocker MapLocker) NodeOptions {
	return readyOptions(func(r *readyHandler) {
		r.txnLocker = txnLocker
	})
}

func WithTLSInfo(tlsInfo transport.TLSInfo) NodeOptions {
	return readyOptions(func(r *readyHandler) {
		r.transport.TLSInfo = tlsInfo
	})
}

func WithDialOptions(options ...grpc.DialOption) NodeOptions {
	return readyOptions(func(r *readyHandler) {
		r.channel.dialOptions = options
	})
}

func WithReadIndexTimeout(d time.Duration) NodeOptions {
	return readyOptions(func(r *readyHandler) {
		r.readIndexTimeout = d
	})
}
