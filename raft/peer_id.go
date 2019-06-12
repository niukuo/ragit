package raft

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

type IP uint32

func ParseIP(str string) (IP, error) {
	ip := net.ParseIP(str)
	if ip != nil {
		ip = ip.To4()
	}
	if ip == nil {
		return 0, fmt.Errorf("invalid ip %s", str)
	}
	return MakeIP(ip[0], ip[1], ip[2], ip[3]), nil
}

func MakeIP(a, b, c, d byte) IP {
	return IP(a) |
		IP(b)<<8 |
		IP(c)<<16 |
		IP(d)<<24
}

func (ip IP) IP() net.IP {
	return net.IPv4(
		byte(ip),
		byte(ip>>8),
		byte(ip>>16),
		byte(ip>>24),
	)
}

func (ip IP) String() string {
	return ip.IP().String()
}

type PeerID uint64

func ParsePeerID(str string) (PeerID, error) {
	s := strings.SplitN(str, ":", 4)
	if len(s) > 3 || len(s) < 2 {
		return 0, errors.New("illegal format")
	}
	ip, err := ParseIP(s[0])
	if err != nil {
		return 0, err
	}
	port, err := strconv.ParseUint(s[1], 10, 16)
	if err != nil {
		return 0, err
	}

	var idx uint64
	if len(s) == 3 {
		idx, err = strconv.ParseUint(s[2], 10, 16)
		if err != nil {
			return 0, err
		}
	}
	return MakeNode(ip, uint16(port), uint16(idx)), nil
}

func MakeNode(ip IP, port uint16, idx uint16) PeerID {
	return PeerID(ip)<<32 | PeerID(idx)<<16 | PeerID(port)
}

func (id PeerID) GetIP() IP {
	return IP(id >> 32)
}

func (id PeerID) GetPort() uint16 {
	return uint16(id)
}

func (id PeerID) GetIndex() uint16 {
	return uint16((id >> 16) & 0xffff)
}

func (id PeerID) String() string {
	return fmt.Sprintf("%v:%v:%v", id.GetIP(), id.GetPort(), id.GetIndex())
}

func (id PeerID) Addr() string {
	return fmt.Sprintf("%v:%v", id.GetIP(), id.GetPort())
}
