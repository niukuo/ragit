package refs

import (
	"fmt"
)

const (
	HashLen = 20
)

type Hash [HashLen]byte

type ReqHandle interface{}

type Listener interface {
	Check(refs map[string]Hash) error
	Apply(oplog Oplog, ctx ReqHandle) error
	FetchObjects(refs map[string]Hash, addrs []string) error
	OnLeaderStart(term uint64)
	OnLeaderStop()
}

func (h Hash) String() string {
	return fmt.Sprintf("%x", h[:])
}
