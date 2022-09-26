package bdb_test

import (
	"bytes"
	"errors"
	"strings"

	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/niukuo/ragit/refs"
)

type emptyListener struct {
}

func (l *emptyListener) Apply(oplog refs.Oplog, handle refs.ReqHandle) error {
	w := &bytes.Buffer{}
	status := packp.ReportStatus{
		UnpackStatus: "ok",
	}
	return status.Encode(w)
}

func (l *emptyListener) Check(map[string]refs.Hash) error {
	return nil
}

func (l *emptyListener) FetchObjects(refMap map[string]refs.Hash, peerURLs []string) error {
	if len(peerURLs) == 0 {
		return errors.New("peer urls cannot be empty")
	}

	if strings.Contains(strings.Join(peerURLs, ","), "8080") { //return error for specific port
		return errors.New("fetchObjects should not be call at when nodeID port is 8080")
	}
	return nil
}

func (l *emptyListener) OnLeaderStart(term uint64) {
}

func (l *emptyListener) OnLeaderStop() {
}
