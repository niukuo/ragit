package refs

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestComputePeerID(t *testing.T) {
	s := assert.New(t)

	addr := []string{"127.0.0.1:9022", "127.0.0.2:9022"}
	memberID := ComputePeerID(addr, nil)
	s.NotZero(memberID)

	curTime := time.Now()
	memberID2 := ComputePeerID(addr, &curTime)
	s.NotEqual(memberID, memberID2)
}

func TestString(t *testing.T) {
	s := assert.New(t)
	id := PeerID(1234)

	s.True(strings.Contains(id.String(), "0"))
	s.EqualValues(len(id.String()), 16)
}
