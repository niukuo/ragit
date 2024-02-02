package api

import (
	"bytes"
	"io"
	"testing"

	fixtures "github.com/go-git/go-git-fixtures/v4"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/stretchr/testify/assert"
)

func TestCheckLimitSize(t *testing.T) {
	s := assert.New(t)
	f := fixtures.Basic().One()
	data, err := io.ReadAll(f.Packfile())
	s.NoError(err)

	h := &httpGitAPI{
		maxFileSize: DefaultMaxFileSize,
		maxPackSize: DefaultMaxPackSize,
	}

	var content []byte

	_, err = h.readPack(bytes.NewReader(nil))
	s.ErrorIs(err, packfile.ErrEmptyPackfile)

	_, err = h.readPack(bytes.NewReader([]byte{}))
	s.ErrorIs(err, packfile.ErrEmptyPackfile)

	content, err = h.readPack(bytes.NewReader(data))
	s.NoError(err)
	s.NotNil(content)

	h.maxFileSize = 10000
	content, err = h.readPack(bytes.NewReader(data))
	s.Error(err)
	s.Nil(content)

	h.maxFileSize = DefaultMaxFileSize
	h.maxPackSize = 10000
	content, err = h.readPack(bytes.NewReader(data))
	s.Error(err)
	s.Nil(content)
}
