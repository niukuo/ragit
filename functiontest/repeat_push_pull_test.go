package functiontest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	"github.com/stretchr/testify/assert"
	etcdraft "go.etcd.io/etcd/raft"
)

func TestRepeatPushPull(t *testing.T) {
	s := assert.New(t)
	localAddr := "http://127.0.0.1:9021"
	peerAddrs := "http://127.0.0.1:9021"
	dir := "./test/"

	node, storage, httpServer := startRagit(s, dir, localAddr, peerAddrs)

	cmdStr := "cd test/; mkdir pullPath;mkdir pushPath;cd pullPath;git init;git remote add origin http://127.0.0.1:9021/repo.git;cd ../pushPath;git init;git remote add origin http://127.0.0.1:9021/repo.git;cd ../../"
	runCmd := exec.Command("/bin/bash", "-c", cmdStr)
	err := runCmd.Run()
	s.NoError(err)

	gitConfigCmd := "cd test/pushPath; git config user.email abc@alibaba.com; git config user.name abc; cd ../pullPath; git config user.email abc@alibaba.com; git config user.name abc"
	runCmd = exec.Command("/bin/bash", "-c", gitConfigCmd)
	err = runCmd.Run()
	s.NoError(err)

	for i := 0; i < 10; i++ {
		pushCmdStr := "cd test/pushPath; echo a >> a.txt; git add a.txt; git commit -m a; git push origin HEAD -f"
		runCmd := exec.Command("/bin/bash", "-c", pushCmdStr)
		stdout, err := runCmd.CombinedOutput()
		s.NoError(err, string(stdout))

		pullCmdStr := "cd test/pullPath; git pull origin master -f"
		runCmd = exec.Command("/bin/bash", "-c", pullCmdStr)
		stdout, err = runCmd.CombinedOutput()
		s.NoError(err, string(stdout))

		diffCmdStr := "diff test/pullPath/a.txt test/pushPath/a.txt"
		runCmd = exec.Command("/bin/bash", "-c", diffCmdStr)
		stdout, err = runCmd.CombinedOutput()
		s.NoError(err, string(stdout))
	}

	closeRagit(dir, node, storage, httpServer)
}

func startRagit(s *assert.Assertions, dir, localAddr, peerAddrs string) (raft.Node, bdb.Storage, *http.Server) {
	myid := refs.ComputePeerID([]string{localAddr}, nil)
	peers := make([]raft.PeerID, 0)
	id := refs.ComputePeerID([]string{peerAddrs}, nil)
	peers = append(peers, id)

	os.RemoveAll(dir)
	listener, err := gitexec.NewListener(path.Join(dir, "repo.git"), logging.GetLogger("gitexec"))
	s.NoError(err)
	opts := bdb.NewOptions()
	opts.Listener = listener
	opts.Logger = logging.GetLogger("")
	storage, err := bdb.Open(dir, opts, func() (refs.PeerID, error) {
		localID := refs.ComputePeerID([]string{localAddr}, nil)
		return localID, nil
	})
	s.NoError(err)
	if hardState, confState, err := storage.InitialState(); err != nil {
		s.NoError(err)
	} else if etcdraft.IsEmptyHardState(hardState) && len(confState.Learners)+len(confState.Voters) == 0 {
		members := make([]*refs.Member, 0)
		peerAddrs := strings.Split(peerAddrs, ",")
		for _, addr := range peerAddrs {
			memberID := refs.ComputePeerID([]string{addr}, nil)
			members = append(members, refs.NewMember(memberID, []string{addr}))
		}
		err := storage.Bootstrap(members)
		s.NoError(err)
	}
	c := raft.NewConfig()
	c.Config = etcdraft.Config{
		ID:                        uint64(myid),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   storage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
		PreVote:                   true,
	}
	c.Storage = storage
	c.StateMachine = storage
	c.LocalAddrs = []string{localAddr}
	node, err := raft.RunNode(c)
	s.NoError(err)
	mux := http.NewServeMux()
	nh := node.Handler()
	mux.Handle("/raft", nh)
	mux.Handle("/raft/", nh)
	mux.Handle("/debug/", http.DefaultServeMux)
	mux.Handle("/refs/", api.NewHandler(storage, node))
	mux.Handle("/repo.git/", http.StripPrefix("/repo.git", api.NewGitHandler("./test/repo.git", storage, node, logging.GetLogger("./test/repo.git"))))
	httpServer := &http.Server{
		Addr:    strings.Trim(localAddr, "http://"),
		Handler: mux,
	}
	go func() {
		err := httpServer.ListenAndServe()
		s.Error(err, http.ErrServerClosed)
	}()

	for {
		memberStatus, err := getMemberStatus(localAddr)
		if err != nil || memberStatus == nil || memberStatus.Lead == "" {
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	return node, storage, httpServer
}

func closeRagit(dir string, node raft.Node, storage bdb.Storage, httpServer *http.Server) {
	rc := node.(raft.ReadyHandler)
	rc.Stop()
	storage.Close()
	httpServer.Shutdown(context.Background())
	os.RemoveAll(dir)
}

func getMemberStatus(addr string) (*raft.MemberStatus, error) {
	url := addr + "/raft/members"
	resp, err := http.DefaultClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster response, url: %v, err: %v", url, err)
	}

	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to read body of cluster response, err: %v", err)
	}

	var memberStatus raft.MemberStatus
	if err := json.Unmarshal(data, &memberStatus); err != nil {
		return nil, fmt.Errorf("failed to unmarshal cluster response, err: %v, memberStatus: %+v", err, memberStatus)
	}

	return &memberStatus, nil
}
