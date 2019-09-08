package refs

import (
	"context"
	"fmt"
	"io"
)

const (
	HashLen = 20
)

type Hash [HashLen]byte

type Listener interface {
	Check(refs map[string]Hash) error
	Apply(ctx context.Context, oplog Oplog, w io.Writer) error
	FetchObjects(refs map[string]Hash, nodeID PeerID) error
	OnLeaderStart(term uint64)
	OnLeaderStop()
}

func (h Hash) String() string {
	return fmt.Sprintf("%x", h[:])
}
