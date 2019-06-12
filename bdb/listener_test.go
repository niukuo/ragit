package bdb_test

import (
	"io"

	"github.com/niukuo/ragit/refs"
	"gopkg.in/src-d/go-git.v4/plumbing/protocol/packp"
)

type emptyListener struct {
}

func (l *emptyListener) Apply(oplog refs.Oplog, w io.Writer) error {
	status := packp.ReportStatus{
		UnpackStatus: "ok",
	}
	return status.Encode(w)
}

func (l *emptyListener) Reset(map[string]refs.Hash) error {
	return nil
}
