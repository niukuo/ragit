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
	ID       PeerID   `json:"id"`
	PeerURLs []string `json:"peer_urls"`
}

func NewMember(id PeerID, peerURLs []string) Member {

	return &member{
		ID:       id,
		PeerURLs: sortPeerURLs(peerURLs),
	}
}

func (m *member) String() string {
	return fmt.Sprintf("{id: %s, peer_urls: %v}", m.ID, m.PeerURLs)
}

func NewMemberID(peerURLs []string, now *time.Time) PeerID {

	str := strings.Join(sortPeerURLs(peerURLs), "")
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

func sortPeerURLs(peerURLs []string) []string {
	if len(peerURLs) == 0 {
		return peerURLs
	}

	us := make([]string, len(peerURLs))
	copy(us, peerURLs)
	sort.Strings(us)

	return us
}
