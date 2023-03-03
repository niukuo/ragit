package api

import (
	"io/ioutil"
	"testing"

	fixtures "github.com/go-git/go-git-fixtures/v4"
	"github.com/stretchr/testify/assert"
)

func TestCheckLimitSize(t *testing.T) {
	s := assert.New(t)
	f := fixtures.Basic().One()
	content, err := ioutil.ReadAll(f.Packfile())
	s.NoError(err)

	h := &httpGitAPI{
		maxFileSize: DefaultMaxFileSize,
		maxPackSize: DefaultMaxPackSize,
	}

	err = h.checkLimitSize(nil)
	s.Error(err)

	err = h.checkLimitSize([]byte{})
	s.Error(err)

	err = h.checkLimitSize(content)
	s.NoError(err)

	h.maxFileSize = 10000
	err = h.checkLimitSize(content)
	s.Error(err)

	h.maxFileSize = DefaultMaxFileSize
	h.maxPackSize = 10000
	err = h.checkLimitSize(content)
	s.Error(err)
}
