package functiontest

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/niukuo/ragit/api"
	"github.com/niukuo/ragit/bdb"
	"github.com/niukuo/ragit/gitexec"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
	"github.com/stretchr/testify/suite"
	etcdraft "go.etcd.io/etcd/raft"
)

type GitServer struct {
	dir      string
	peerURLs []string

	storage bdb.Storage
	node    raft.Node
}

func NewGitServer(dir string, peerURLs []string) *GitServer {
	return &GitServer{
		dir:      dir,
		peerURLs: peerURLs,
	}
}

func (s *GitServer) Start() error {

	listener, err := gitexec.NewListener(
		path.Join(s.dir, "repo.git"), logging.GetLogger("gitexec"),
	)
	if err != nil {
		return err
	}

	myid := refs.NewMemberID(s.peerURLs, nil)

	opts := bdb.NewOptions()
	opts.Listener = listener
	opts.Logger = logging.GetLogger("")
	opts.NewLocalID = func() refs.PeerID {
		return refs.NewMemberID(s.peerURLs, nil)
	}

	s.storage, err = bdb.Open(s.dir, opts)
	if err != nil {
		return err
	}

	if hardState, confState, err := s.storage.InitialState(); err != nil {
		return err
	} else if etcdraft.IsEmptyHardState(hardState) &&
		len(confState.Learners)+len(confState.Voters) == 0 {
		members := []refs.Member{
			{
				ID:       myid,
				PeerURLs: s.peerURLs,
			},
		}
		if err := s.storage.Bootstrap(members); err != nil {
			return err
		}
	}

	c := raft.NewConfig()
	c.Config = etcdraft.Config{
		ID:                        uint64(myid),
		ElectionTick:              3,
		HeartbeatTick:             1,
		Storage:                   s.storage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
		PreVote:                   true,
	}

	c.Storage = s.storage
	c.StateMachine = s.storage
	c.PeerListenURLs = s.peerURLs

	s.node, err = raft.RunNode(c)
	if err != nil {
		return err
	}

	s.becomeLeader()

	mux := http.NewServeMux()
	nh := s.node.Handler()
	mux.Handle("/raft", nh)
	mux.Handle("/raft/", nh)
	mux.Handle("/debug/", http.DefaultServeMux)
	mux.Handle("/refs/", api.NewHandler(s.storage, s.node))
	mux.Handle("/repo.git/",
		http.StripPrefix("/repo.git",
			api.NewGitHandler(
				path.Join(s.dir, "repo.git"),
				s.storage, s.node, logging.GetLogger("git.op"))))

	httpServer := &http.Server{
		Addr:    strings.TrimPrefix(s.peerURLs[0], "http://"),
		Handler: mux,
	}

	go func() {
		err := httpServer.ListenAndServe()
		log.Fatal("http.ListenAndServe err: ", err)
	}()

	return nil
}

func (s *GitServer) becomeLeader() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for {
		select {
		case <-time.After(time.Second):
			if s.storage.GetLeaderTerm() != 0 {
				log.Println("became leader")
				return
			}

		case <-ctx.Done():
			panic(fmt.Sprintf("wait err: %s", ctx.Err()))
		}
	}
}

func (s *GitServer) Stop() {
	s.node.(raft.ReadyHandler).Stop()
	s.storage.Close()
	os.RemoveAll(s.dir)
}

type GitServerFT struct {
	suite.Suite
	gitServer *GitServer
}

func TestGitServerFT(t *testing.T) {
	suite.Run(t, new(GitServerFT))
}

func (s *GitServerFT) SetupSuite() {

	dir, err := ioutil.TempDir("", "ragit_test")
	s.NoError(err)

	s.gitServer = NewGitServer(dir, []string{"http://127.0.0.1:9022"})

	s.NoError(s.gitServer.Start())
}

func (s *GitServerFT) TearDownSuite() {
	s.gitServer.Stop()
}

func (s *GitServerFT) TestRepeatPushAndPull() {
	dataDir, err := ioutil.TempDir("", "data")
	s.NoError(err)
	defer os.RemoveAll(dataDir)

	initRepo := "mkdir pullPath;mkdir pushPath;cd pullPath;git init;git remote add origin http://127.0.0.1:9022/repo.git;cd ../pushPath;git init;git remote add origin http://127.0.0.1:9022/repo.git"
	cmd := exec.Command("/bin/bash", "-c", initRepo)
	cmd.Dir = dataDir
	s.NoError(cmd.Run())

	gitConfig := "cd pushPath; git config user.email abc@alibaba.com; git config user.name abc; cd ../pullPath; git config user.email abc@alibaba.com; git config user.name abc"
	cmd = exec.Command("/bin/bash", "-c", gitConfig)
	cmd.Dir = dataDir
	s.NoError(cmd.Run())

	for i := 0; i < 10; i++ {
		push := "cd pushPath; echo a >> a.txt; git add a.txt; git commit -m a; git push origin HEAD"
		cmd := exec.Command("/bin/bash", "-c", push)
		cmd.Dir = dataDir
		stdout, err := cmd.CombinedOutput()
		s.NoError(err, string(stdout))

		pull := "cd pullPath; git pull origin master"
		cmd = exec.Command("/bin/bash", "-c", pull)
		cmd.Dir = dataDir
		stdout, err = cmd.CombinedOutput()
		s.NoError(err, string(stdout))

		diff := "diff pullPath/a.txt pushPath/a.txt"
		cmd = exec.Command("/bin/bash", "-c", diff)
		cmd.Dir = dataDir
		stdout, err = cmd.CombinedOutput()
		s.NoError(err, string(stdout))
	}
}
