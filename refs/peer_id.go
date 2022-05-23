package refs

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"time"
)

type PeerID uint64

func (id PeerID) String() string {
	return fmt.Sprintf("%016x", uint64(id))
}

func ComputePeerID(addrs []string, now *time.Time) PeerID {
	sort.Strings(addrs)
	joinedAddrs := strings.Join(addrs, "")
	b := []byte(joinedAddrs)
	if now != nil {
		b = append(b, []byte(fmt.Sprintf("%d", now.Unix()))...)
	}

	hash := sha1.Sum(b)
	return PeerID(binary.BigEndian.Uint64(hash[:8]))
}
