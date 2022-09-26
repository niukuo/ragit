package refs

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"time"
)

type Member = *member
type member struct {
	ID       PeerID
	PeerURLs []string
}

func NewMember(id PeerID, peerURLs []string) Member {
	return &member{
		ID:       id,
		PeerURLs: peerURLs,
	}
}

func (m *member) String() string {
	return fmt.Sprintf("{id: %s, peer_urls: %v}", m.ID, m.PeerURLs)
}

func NewMemberID(peerURLs []string, now *time.Time) PeerID {

	sort.Strings(peerURLs)

	str := strings.Join(peerURLs, "")
	b := []byte(str)
	if now != nil {
		b = append(b, []byte(fmt.Sprintf("%d", now.Unix()))...)
	}

	hash := sha1.Sum(b)
	return PeerID(binary.BigEndian.Uint64(hash[:8]))
}

func DefaultNewMemberID(peerURLs []string) PeerID {
	now := time.Now()
	return NewMemberID(peerURLs, &now)
}
