package raft

import (
	"github.com/niukuo/ragit/refs"
)

var (
	ParseIP     = refs.ParseIP
	MakeIP      = refs.MakeIP
	ParsePeerID = refs.ParsePeerID
	MakeNode    = refs.MakeNode
)

type (
	IP     = refs.IP
	PeerID = refs.PeerID
)
