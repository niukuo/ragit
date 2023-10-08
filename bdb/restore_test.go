package bdb_test

import (
	"os"
	"path"
	"testing"

	"github.com/niukuo/ragit/bdb"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/suite"
)

type restoreSuite struct {
	suite.Suite
	dir string
}

func TestRestore(t *testing.T) {
	suite.Run(t, &restoreSuite{})
}

func (s *restoreSuite) SetupTest() {
	dir, err := os.MkdirTemp("", "restore_test")
	s.NoError(err)

	s.dir = dir
}

func (s *restoreSuite) TearDownTest() {
	s.NoError(os.RemoveAll(s.dir))
}

func (s *restoreSuite) TestRestore() {
	opts := bdb.NewOptions()
	opts.Logger = logging.GetLogger("")
	opts.NewLocalID = func() (refs.PeerID, error) {
		return refs.PeerID(123456), nil
	}
	storage, err := bdb.Open(path.Join(s.dir, "datadir"), opts)
	s.NoError(err)
	members := []refs.Member{
		refs.NewMember(refs.PeerID(111), []string{"http://127.0.0.1:2023"}),
		refs.NewMember(refs.PeerID(111), []string{"http://127.0.0.1:2024"}),
	}
	s.NoError(storage.Bootstrap(members))
	storage.Close()

	// restore
	mgr := bdb.NewRestorer(bdb.WithLogger(logging.GetLogger("restore_test")))
	cfg := bdb.RestoreConfig{
		DataDir:       path.Join(s.dir, "datadir"),
		OutputDataDir: path.Join(s.dir, "outdatadir"),

		LocalID: refs.PeerID(123),
		Members: []refs.Member{
			refs.NewMember(refs.PeerID(111), []string{"http://127.0.0.1:2025"}),
			refs.NewMember(refs.PeerID(112), []string{"http://127.0.0.1:2026"}),
		},
	}
	s.NoError(mgr.Restore(cfg))

	status, err := mgr.Status(path.Join(s.dir, "outdatadir", "state.db"))
	s.NoError(err)
	s.EqualValues(1, status.AppliedIndex)
	s.EqualValues(1, status.ConfIndex)
	s.EqualValues(123, uint64(status.LocalID))
	s.EqualValues([]uint64{111, 112}, status.ConfState.Voters)
	s.Equal(2, len(status.Members))
}

func (s *restoreSuite) TestCopyFile() {
	src := path.Join(s.dir, "src")
	dst := path.Join(s.dir, "dst")
	s.NoError(os.Mkdir(src, 0755))
	s.NoError(os.Mkdir(dst, 0755))

	srcFile := path.Join(src, "file1")
	dstFile := path.Join(dst, "file1")
	s.NoError(os.WriteFile(srcFile, []byte("file1"), 0644))
	s.NoError(bdb.CopyFile(srcFile, dstFile))

	s.FileExists(dstFile)
	info, err := os.Stat(dstFile)
	s.NoError(err)
	s.EqualValues(0644, info.Mode().Perm())
}
