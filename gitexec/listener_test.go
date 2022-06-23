package gitexec

import (
	"os"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/assert"
)

func TestFetchObjects(t *testing.T) {
	s := assert.New(t)
	listener, err := NewListener("test_fetch/", logging.GetLogger("test_fetch"))
	s.NoError(err)

	s.NoError(listener.FetchObjects(map[string]refs.Hash{}, []string{"http://127.0.0.1:2022"}))
}

func TestSetRefs(t *testing.T) {
	s := assert.New(t)

	os.RemoveAll("test_fetch/")
	listener, err := NewListener("test_fetch/test.git", logging.GetLogger("test_fetch"))
	s.NoError(err)

	refsMap := make(map[string]refs.Hash)
	var hash1 refs.Hash
	copy(hash1[:], "a78d81f0c704ddbf1496bbd821555014fe211b3e")
	var hash2 refs.Hash
	copy(hash2[:], "73ee175298ef045f8ba6f82542890e223f383a33")
	refsMap["refs/heads/master"] = hash1
	refsMap["refs/heads/test"] = hash2
	err = listener.setRefs(refsMap)
	s.NoError(err)

	iter, err := listener.store.IterReferences()
	s.NoError(err)

	err = iter.ForEach(func(ref *plumbing.Reference) error {
		name := string(ref.Name())
		if name == string(plumbing.HEAD) {
			return nil
		}

		if _, ok := refsMap[name]; !ok {
			refsMap[name] = refs.Hash{}
		}
		return nil
	})
	s.NoError(err)

	os.RemoveAll("test_fetch/")
}

func TestCheckAndBuildCmd(t *testing.T) {
	s := assert.New(t)

	os.RemoveAll("test_fetch/")
	l, err := NewListener("test_fetch/test.git", logging.GetLogger("test_fetch"))
	s.NoError(err)

	refsMap := make(map[string]refs.Hash)
	var hash1 refs.Hash
	copy(hash1[:], "a78d81f0c704ddbf1496bbd821555014fe211b3e")
	var hash2 refs.Hash
	copy(hash2[:], "73ee175298ef045f8ba6f82542890e223f383a33")
	refsMap["refs/heads/master"] = hash1
	refsMap["refs/heads/test"] = hash2
	wants := make(map[plumbing.Hash]bool)
	haves := make(map[plumbing.Hash]bool)
	cmds := make(map[plumbing.ReferenceName]*packp.Command)
	var hash3 refs.Hash
	copy(hash3[:], "0000000000000000000000000000000000000000")
	for remoteRef, hash := range refsMap {
		err := checkAndBuildCmd(l.store,
			plumbing.ReferenceName(remoteRef), plumbing.Hash(hash),
			wants, haves, cmds)
		s.NoError(err)
		s.True(wants[plumbing.Hash(hash)])
		s.NotNil(cmds[plumbing.ReferenceName(remoteRef)])
		s.NotNil(cmds[plumbing.ReferenceName(remoteRef)].Old)
	}

	os.RemoveAll("test_fetch/")
}
