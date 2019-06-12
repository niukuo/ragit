package refs_test

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/assert"
)

func TestValidate(t *testing.T) {
	s := assert.New(t)

	opInvalid1 := refs.Oplog{
		Ops: []*refs.Oplog_Op{
			&refs.Oplog_Op{
				Name:   proto.String("master"),
				Target: []byte("1234567890abcdef1234"),
			},
		},
	}

	opInvalid2 := refs.Oplog{
		Ops: []*refs.Oplog_Op{
			&refs.Oplog_Op{
				Name:   proto.String("refs/heads/master"),
				Target: []byte("1234567890abcdef"),
			},
		},
	}

	s.Error(refs.Validate(opInvalid1))
	s.Error(refs.Validate(opInvalid2))
}
