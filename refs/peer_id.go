package refs

import (
	"fmt"
)

type PeerID uint64

type FormatFunc = func(PeerID) string

var formatter FormatFunc

func Register(f FormatFunc) {
	if formatter != nil {
		panic("formatter has been provided")
	}
	formatter = f
}

func (id PeerID) String() string {
	return fmt.Sprintf("%016x", uint64(id))
}

func (id PeerID) Format() string {
	if formatter == nil {
		return id.String()
	}
	return formatter(id)
}
