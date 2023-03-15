package gitexec

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"sort"
	"syscall"
	"time"

	"github.com/go-git/go-billy/v5/osfs"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/cache"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/sideband"
	"github.com/go-git/go-git/v5/plumbing/storer"
	"github.com/go-git/go-git/v5/plumbing/transport"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/storage/filesystem"
	"github.com/go-git/go-git/v5/utils/ioutil"
	"github.com/juju/errors"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
)

// Listener executes git command during apply
type Listener = *listener
type listener struct {
	dir       string
	store     storer.Storer
	gitClient transport.Transport
	logger    logging.Logger
}

type Options func(l *listener)

func WithGitClient(gitClient transport.Transport) Options {
	return Options(func(l *listener) {
		l.gitClient = gitClient
	})
}

// NewListener return a listener for the repo
func NewListener(dir string, logger logging.Logger, opts ...Options) (Listener, error) {

	l := &listener{
		dir:       dir,
		gitClient: githttp.DefaultClient,
		logger:    logger,
	}
	l.store = filesystem.NewStorageWithOptions(osfs.New(dir),
		cache.NewObjectLRUDefault(), filesystem.Options{})

	for _, opt := range opts {
		opt(l)
	}

	return l, nil
}

func (l *listener) Apply(oplog refs.Oplog, handle refs.ReqHandle) error {

	start := time.Now()

	cmd := exec.Command("git", "receive-pack", "--stateless-rpc", ".")
	cmd.Dir = l.dir
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGTERM,
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer

	var wbuf io.Writer = &stdout
	if ch, ok := handle.(WriterCh); ok {
		if w, ok := <-ch; ok {
			wbuf = io.MultiWriter(wbuf, w)
		}
	}

	cmd.Stdout = wbuf
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

	if _, err := bytes.NewReader(oplog.ObjPack).WriteTo(stdin); err != nil {
		return errors.Annotate(err, "write packfile to stdin err")
	}

	if err := stdin.Close(); err != nil {
		return errors.Annotate(err, "close stdin err")
	}

	if err := cmd.Wait(); err != nil {
		return errors.Annotate(err, "cmd Wait err")
	}

	if stderr.Len() > 0 {
		l.logger.Warning("stderr: \n", stderr.String())
	}

	var status packp.ReportStatus
	if err := status.Decode(bytes.NewReader(stdout.Bytes())); err != nil {
		l.logger.Warning("decode report status err: ", err,
			", out: \n", stdout.String())
		return err
	}

	if err := status.Error(); err != nil {
		l.logger.Warning("refs not updated, err: ", err)
		return err
	}

	applySeconds.Observe(time.Since(start).Seconds())

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

func (l *listener) FetchObjects(refsMap map[string]refs.Hash, addrs []string) (err error) {
	var endpoint *transport.Endpoint
	for _, addr := range addrs {
		epoint, err := transport.NewEndpoint(addr + "/" + path.Base(l.dir))
		if err == nil {
			endpoint = epoint
			break
		}
		l.logger.Warningf("[listener.FetchObjects] transport NewEndpoint failed, err: %v, addr: %v", err, addr)
	}
	if endpoint == nil {
		return fmt.Errorf("[listener.FetchObjects] transport NewEndpoint failed, addrs: %v", addrs)
	}

	s, err := l.gitClient.NewUploadPackSession(endpoint, nil)
	if err != nil {
		return err
	}
	defer ioutil.CheckClose(s, &err)

	wants := make(map[plumbing.Hash]bool)
	haves := make(map[plumbing.Hash]bool)

	for remoteRef, hash := range refsMap {
		if err := checkAndBuildCmd(l.store,
			plumbing.ReferenceName(remoteRef), plumbing.Hash(hash),
			wants, haves, make(map[plumbing.ReferenceName]*packp.Command)); err != nil {
			return err
		}
	}

	if len(wants) > 0 {
		req := packp.NewUploadPackRequest()
		if err := req.Capabilities.Set(capability.OFSDelta); err != nil {
			return err
		}
		if err := req.Capabilities.Set(capability.NoProgress); err != nil {
			return err
		}
		if err := req.Capabilities.Set(capability.Sideband64k); err != nil {
			return err
		}

		for hash := range wants {
			req.Wants = append(req.Wants, hash)
		}
		for hash := range haves {
			req.Haves = append(req.Haves, hash)
		}

		if err = l.fetchObjects(context.Background(), s, req); err != nil {
			l.logger.Warning("[listener.FetchObjects] fetch pack failed, err: ", err)
			return err
		}
	}

	iter, err := l.store.IterReferences()
	if err != nil {
		l.logger.Warning("iter refs failed: ", err)
		return err
	}
	defer iter.Close()

	if err := iter.ForEach(func(ref *plumbing.Reference) error {
		name := string(ref.Name())
		if name == string(plumbing.HEAD) {
			return nil
		}

		if _, ok := refsMap[name]; !ok {
			refsMap[name] = refs.Hash{}
		}
		return nil
	}); err != nil {
		l.logger.Warning("iter refs failed: ", err)
		return err
	}

	err = l.setRefs(refsMap)

	l.logger.Info("[listener.FetchObjects] [End] from ", addrs)

	return err
}

func (l *listener) fetchObjects(ctx context.Context,
	s transport.UploadPackSession,
	req *packp.UploadPackRequest,
) (err error) {
	start := time.Now()

	plumbing.HashesSort(req.Wants)
	plumbing.HashesSort(req.Haves)

	l.logger.Info("[FetchObjects] fetching pack, wants: ",
		req.Wants, ", haves: ", req.Haves)

	reader, err := s.UploadPack(ctx, req)
	if err != nil {
		return err
	}
	defer ioutil.CheckClose(reader, &err)

	sr := buildSidebandIfSupported(req.Capabilities, reader, nil)

	if err := packfile.UpdateObjectStorage(l.store, sr); err != nil {
		l.logger.Warning("[FetchObjects] save pack failed, err: ", err)
		return err
	}

	fetchObjectsSeconds.Observe(time.Since(start).Seconds())

	return nil
}

func (l *listener) setRefs(refsMap map[string]refs.Hash) error {
	l.logger.Info("[listener.setRefs] [Begin] refMap: ", refsMap)
	var empty refs.Hash
	for refName, hash := range refsMap {
		name := plumbing.ReferenceName(refName)
		var err error
		if hash == empty {
			err = l.store.RemoveReference(name)
		} else {
			r := plumbing.NewHashReference(name, plumbing.Hash(hash))
			err = l.store.SetReference(r)
		}
		if err != nil {
			return fmt.Errorf("fail to set ref %s to %s, err: %v", refName, hash, err)
		}
	}

	l.logger.Info("[listener.setRefs] [End]")
	return nil
}

func (l *listener) OnLeaderStart(term uint64) {
}

func (l *listener) OnLeaderStop() {
}

func checkAndBuildCmd(rstore storer.Storer,
	localRefName plumbing.ReferenceName, hash plumbing.Hash,
	wants, haves map[plumbing.Hash]bool,
	cmds map[plumbing.ReferenceName]*packp.Command) error {
	cmd := &packp.Command{
		Name: localRefName,
		New:  hash,
	}

	switch localRef, err := storer.ResolveReference(rstore, localRefName); err {
	case nil:
		if localRef.Hash() == hash {
			return nil
		}
		if err := rstore.HasEncodedObject(hash); err != nil {
			wants[hash] = true
			haves[localRef.Hash()] = true
		}
		cmd.Old = localRef.Hash()
	case plumbing.ErrReferenceNotFound:
		wants[hash] = true
	default:
		return err
	}

	if oldCmd, ok := cmds[localRefName]; ok && oldCmd != cmd {
		return fmt.Errorf("local ref %s has duplicated src", localRefName)
	}
	cmds[localRefName] = cmd
	return nil
}

func buildSidebandIfSupported(l *capability.List, reader io.Reader, p sideband.Progress) io.Reader {
	var t sideband.Type

	switch {
	case l.Supports(capability.Sideband):
		t = sideband.Sideband
	case l.Supports(capability.Sideband64k):
		t = sideband.Sideband64k
	default:
		return reader
	}

	d := sideband.NewDemuxer(t, reader)
	d.Progress = p

	return d
}
