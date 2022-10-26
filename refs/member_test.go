package refs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMember(t *testing.T) {
	s := assert.New(t)

	peerUrls := []string{"http://127.0.0.2:9021", "http://127.0.0.1:9021"}
	m := NewMember(DefaultNewMemberID(peerUrls), peerUrls)

	s.Equal(m.PeerURLs[0], peerUrls[1])
	s.Equal(m.PeerURLs[1], peerUrls[0])
}

func TestNewMemberID(t *testing.T) {
	s := assert.New(t)

	urls := []string{"http://127.0.0.1:9022", "http://127.0.0.2:9022"}
	id1 := NewMemberID(urls, nil)
	s.Equal("4bcd9049ba12a4b7", id1.String())

	id2 := NewMemberID(urls, nil)
	s.Equal(id1, id2)

	cur := time.Now()
	id3 := NewMemberID(urls, &cur)
	s.NotEqual(id1, id3)

	next := cur.Add(time.Second)
	id4 := NewMemberID(urls, &next)
	s.NotEqual(id3, id4)
}
