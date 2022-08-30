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

func (l *emptyListener) FetchObjects(refMap map[string]refs.Hash, addrs []string) error {
	if len(addrs) == 0 {
		return errors.New("nodeID should not be zero")
	}

	if strings.Contains(strings.Join(addrs, ","), "8080") { //return error for specific port
		return errors.New("fetchObjects should not be call at when nodeID port is 8080")
	}
	return nil
}

func (l *emptyListener) OnLeaderStart(term uint64) {
}

func (l *emptyListener) OnLeaderStop() {
}
