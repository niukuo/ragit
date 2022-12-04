package refs

import (
	"fmt"
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

	formatInt := func(id PeerID) string {
		return fmt.Sprintf("%d", uint64(id))
	}
	Register(formatInt)
	s.Equal("00000000000004d2", id.String())
	s.Equal("1234", id.Format())
}
