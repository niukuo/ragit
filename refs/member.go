package refs

import "fmt"

type Member struct {
	ID        PeerID
	PeerAddrs []string
}

func NewMember(id PeerID, peerAddrs []string) *Member {
	return &Member{
		ID:        id,
		PeerAddrs: peerAddrs,
	}
}

func (m *Member) String() string {
	return fmt.Sprintf("{ID: %v, PeerAddrs: %v}", m.ID, m.PeerAddrs)
}
