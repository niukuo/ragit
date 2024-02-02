package gitexec

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
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
	"github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
)

var flag_SkipNextErrEntry = flag.Bool("ragit.skip_next_err_entry", false,
	"skip next error entry, only valid for the first one")

// Listener executes git command during apply
type Listener = *listener
type listener struct {
	dir       string
	store     storer.Storer
	gitClient transport.Transport
	logger    logging.Logger
}

var _ refs.Listener = (*listener)(nil)

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

func (l *listener) Apply(cmds []*packp.Command, packfile []byte, handle refs.ReqHandle) error {

	start := time.Now()

	cmd := exec.Command("git", "receive-pack", "--stateless-rpc", ".")
	cmd.Dir = l.dir
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGTERM,
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	var statusReader io.Reader = stdout

	req := packp.NewReferenceUpdateRequest()
	req.Commands = cmds
	req.Packfile = io.NopCloser(bytes.NewReader(packfile))
	caps := []capability.Capability{
		capability.Atomic,
		capability.ReportStatus,
	}

	if ch, ok := handle.(ReqCh); ok {
		if oreq, ok := <-ch; ok {
			var d *sideband.Demuxer
			if oreq.Capabilities.Supports(capability.Sideband64k) {
				d = sideband.NewDemuxer(sideband.Sideband64k, stdout)
				caps = append(caps, capability.Sideband64k)
			} else if oreq.Capabilities.Supports(capability.Sideband) {
				d = sideband.NewDemuxer(sideband.Sideband, stdout)
				caps = append(caps, capability.Sideband)
			}
			if d != nil {
				l.logger.Info("apply with sideband progress",
					", 64k: ", oreq.Capabilities.Supports(capability.Sideband64k),
				)
				// shouldn't write before channel closed
				<-ch
				d.Progress = oreq.Progress
				statusReader = d
			}

		}
	}

	for _, cap := range caps {
		if err := req.Capabilities.Set(cap); err != nil {
			return err
		}
	}

	if err := cmd.Start(); err != nil {
		return errors.Annotate(err, "start err")
	}

	if err := req.Encode(stdin); err != nil {
		return errors.Annotate(err, "encode err")
	}
	if err := stdin.Close(); err != nil {
		l.logger.Warning("close stdin failed",
			", err: ", err,
		)
	}

	statusData, err := io.ReadAll(statusReader)
	if err != nil {
		return err
	}

	var status packp.ReportStatus
	decodeErr := status.Decode(bytes.NewReader(statusData))

	waitErr := cmd.Wait()

	if stderr.Len() > 0 {
		l.logger.Warning("stderr: \n", stderr.String())
	}

	if err := waitErr; err != nil {
		l.logger.Warning("process exited unexpectedly, err: ", err)
		return errors.Annotate(err, "cmd Wait err")
	}

	if err := decodeErr; err != nil {
		l.logger.Warning("decode report status err: ", err, ", status: ", string(statusData))
		return err
	}

	canSkip := *flag_SkipNextErrEntry
	*flag_SkipNextErrEntry = false

	if err := status.Error(); err != nil {
		if canSkip {
			l.logger.Info("refs not updated and skipped, err: ", err)
			err = raft.ErrSkipEntry()
		} else {

			l.logger.Warning("refs not updated, err: ", err)
		}
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
