package gitexec

import (
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/niukuo/ragit/refs"
)

// should be closed after send
type ReqCh <-chan *packp.ReferenceUpdateRequest

var _ refs.ReqHandle = ReqCh(nil)
