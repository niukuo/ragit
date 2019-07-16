package refs

import (
	"fmt"
	"io"
)

const (
	HashLen = 20
)

type Hash [HashLen]byte

type Listener interface {
	Apply(oplog Oplog, w io.Writer) error
	Reset(refs map[string]Hash) error
	FetchObjects(refs map[string]Hash, nodeID PeerID) error
}

func (h Hash) String() string {
	return fmt.Sprintf("%x", h[:])
}
