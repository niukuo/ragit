package bdb

import (
	"testing"

	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/assert"
)

func TestIPv4ToHttpAddr(t *testing.T) {
	s := assert.New(t)

	var id uint64 = 680414508648243091
	s.Equal("0971516400001f93", refs.PeerID(id).String())
	s.Equal("http://100.81.113.9:8083", ipv4ToHttpAddr(id))
}
