package refs

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPeerID(t *testing.T) {
	s := assert.New(t)
	id := PeerID(1234)

	s.True(strings.Contains(id.String(), "0"))
	s.EqualValues(len(id.String()), 16)
	s.Equal("00000000000004d2", id.String())
}
