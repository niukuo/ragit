package refs

import (
	"fmt"

	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
)

const (
	HashLen = 20
)

type Hash [HashLen]byte

type ReqHandle interface{}

type Listener interface {
	Check(refs map[string]Hash) error
	Apply(cmds []*packp.Command, packfile []byte, handle ReqHandle) error
	FetchObjects(refs map[string]Hash, addrs []string) error
	OnLeaderStart(term uint64)
	OnLeaderStop()
}

func (h Hash) String() string {
	return fmt.Sprintf("%x", h[:])
}
