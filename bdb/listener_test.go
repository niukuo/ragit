package bdb_test

import (
	"context"
	"errors"
	"io"

	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/niukuo/ragit/refs"
)

type emptyListener struct {
}

func (l *emptyListener) Apply(ctx context.Context, oplog refs.Oplog, w io.Writer) error {
	status := packp.ReportStatus{
		UnpackStatus: "ok",
	}
	return status.Encode(w)
}

func (l *emptyListener) Check(map[string]refs.Hash) error {
	return nil
}

func (l *emptyListener) FetchObjects(refMap map[string]refs.Hash, nodeID refs.PeerID) error {
	if nodeID == 0 {
		return errors.New("nodeID should not be zero")
	}

	port := nodeID.GetPort()
	if port == 8080 { //return error for specific port
		return errors.New("fetchObjects should not be call at when nodeID port is 8080")
	}
	return nil
}

func (l *emptyListener) OnLeaderStart(term uint64) {
}

func (l *emptyListener) OnLeaderStop() {
}
