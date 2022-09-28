package refs

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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

func TestPeerID(t *testing.T) {
	s := assert.New(t)
	id := PeerID(1234)

	s.True(strings.Contains(id.String(), "0"))
	s.EqualValues(len(id.String()), 16)
	s.Equal("00000000000004d2", id.String())
}
