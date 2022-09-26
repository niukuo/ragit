package refs

import (
	"fmt"
)

type PeerID uint64

func (id PeerID) String() string {
	return fmt.Sprintf("%016x", uint64(id))
}
