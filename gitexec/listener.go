package gitexec

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"syscall"

	"github.com/juju/errors"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/format/pktline"
	"gopkg.in/src-d/go-git.v4/plumbing/protocol/packp/capability"
)

// Listener executes git command during apply
type Listener = *listener
type listener struct {
	dir    string
	logger logging.Logger
}

// NewListener return a listener for the repo
func NewListener(dir string, logger logging.Logger) (Listener, error) {

	l := &listener{
		dir:    dir,
		logger: logger,
	}
	return l, nil
}

func (l *listener) Apply(ctx context.Context, oplog refs.Oplog, w io.Writer) error {

	cmd := exec.Command("git", "receive-pack", "--stateless-rpc", ".")
	cmd.Dir = l.dir
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGTERM,
	}
	var stderr bytes.Buffer
	cmd.Stdout = w
	cmd.Stderr = &stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return errors.Annotate(err, "stdin pipe err")
	}

	if err := cmd.Start(); err != nil {
		return errors.Annotate(err, "start err")
	}

	e := pktline.NewEncoder(stdin)

	formatCommand := func(op *refs.Oplog_Op) string {
		var o, n plumbing.Hash
		copy(o[:], op.GetOldTarget())
		copy(n[:], op.GetTarget())
		return fmt.Sprintf("%s %s %s", o, n, op.GetName())
	}

	cap := capability.NewList()
	cap.Set(capability.Atomic)
	cap.Set(capability.ReportStatus)

	if err := e.Encodef("%s\x00%s",
		formatCommand(oplog.Ops[0]), cap.String()); err != nil {
		return err
	}

	for _, cmd := range oplog.Ops[1:] {
		if err := e.Encodef(formatCommand(cmd)); err != nil {
			return err
		}
	}

	if err := e.Flush(); err != nil {
		return err
	}

	if _, err := io.Copy(stdin, bytes.NewReader(oplog.ObjPack)); err != nil {
		return errors.Annotate(err, "write packfile to stdin err")
	}

	if err := stdin.Close(); err != nil {
		return errors.Annotate(err, "close stdin err")
	}

	if err := cmd.Wait(); err != nil {
		return errors.Annotate(err, "close stdin err")
	}

	if stderr.Len() > 0 {
		l.logger.Warning("stderr: \n", stderr.String())
	}

	return nil

}

func (l *listener) setConf(config map[string]string) error {

	keys := make([]string, 0, len(config))
	for k := range config {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if err := l.exec("git", "config", key, config[key]); err != nil {
			return err
		}
	}

	return nil
}

func (l *listener) checkRefs(refsMap map[string]refs.Hash) error {

	output, err := l.execOutput("git", "show-ref")
	if err != nil {
		if err, ok := err.(*exec.ExitError); len(refsMap) > 0 || !ok || exitCode(err.ProcessState) != 1 {
			return errors.Annotatef(err, "show-ref failed, stdout: %s", string(output))
		}
		if len(output) == 0 {
			return nil
		}
		return err
	}

	lines := bytes.Split(bytes.TrimSpace(output), []byte("\n"))
	repoRefs := make(map[string]refs.Hash)
	for _, line := range lines {
		slices := bytes.SplitN(line, []byte(" "), 3)
		if len(slices) != 2 {
			return fmt.Errorf("invalid line: %s", line)
		}

		if len(slices[0]) != 40 {
			return fmt.Errorf("invalid hash for %s: %s", slices[1], slices[0])
		}

		var hash refs.Hash
		if _, err := hex.Decode(hash[:], slices[0]); err != nil {
			return err
		}

		repoRefs[string(slices[1])] = hash
	}

	for name, expHash := range refsMap {
		if hash, ok := repoRefs[name]; !ok {
			return fmt.Errorf("missing ref %s in repo", name)
		} else if hash != expHash {
			return fmt.Errorf(
				"hash mismatch for %s: %x(actual) != %x(expected)", name, hash, expHash)
		}
		delete(repoRefs, name)
	}

	for name := range repoRefs {
		return fmt.Errorf("repo has unexpected ref: %s", name)
	}

	return nil
}

func (l *listener) Check(refsMap map[string]refs.Hash) error {

	if err := os.MkdirAll(l.dir, 0755); err != nil {
		return err
	}

	if err := l.exec("git", "init", "--bare"); err != nil {
		return err
	}

	config := map[string]string{
		"core.logAllRefUpdates":     "true",
		"receive.denyDeleteCurrent": "false",
	}

	if err := l.setConf(config); err != nil {
		return err
	}

	if err := l.checkRefs(refsMap); err != nil {
		return err
	}

	return nil
}

func (l *listener) execOutput(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	cmd.Dir = l.dir
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGTERM,
	}

	return cmd.Output()
}

func (l *listener) exec(name string, args ...string) error {
	if output, err := l.execOutput(name, args...); err != nil {
		return errors.Annotatef(err,
			"err running %s %v, stderr: %s", name, args, string(output))
	}

	return nil
}

func (l *listener) FetchObjects(refs map[string]refs.Hash, nodeID refs.PeerID) error {
	//TODO: POST $GIT_URL/git-upload-pack HTTP/1.0
	return errors.New("not implemented")
}

func (l *listener) OnLeaderStart(term uint64) {
}

func (l *listener) OnLeaderStop() {
}
