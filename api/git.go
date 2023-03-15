package api

/*
https://github.com/git/git/blob/master/Documentation/technical/http-protocol.txt
*/

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os/exec"
	"path"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/niukuo/ragit/bdb"
	"github.com/niukuo/ragit/gitexec"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
)

type Service int

const (
	Service_Invalid Service = iota
	Service_UploadPack
	Service_ReceivePack
)

type Options func(h *httpGitAPI)

func WithLimitSize(maxFileSize, maxPackSize int64) Options {
	return Options(func(h *httpGitAPI) {
		h.maxFileSize = maxFileSize
		h.maxPackSize = maxPackSize
	})
}

const (
	DefaultMaxFileSize = 1 * 1024 * 1024
	DefaultMaxPackSize = 10 * 1024 * 1024
)

type httpGitAPI struct {
	path string
	rdb  Storage
	node raft.Node
	*http.ServeMux

	maxFileSize int64
	maxPackSize int64

	logger logging.Logger
}

func NewGitHandler(
	path string,
	rdb Storage,
	node raft.Node,
	logger logging.Logger,
	opts ...Options,
) *httpGitAPI {
	h := &httpGitAPI{
		path:        path,
		rdb:         rdb,
		node:        node,
		maxFileSize: DefaultMaxFileSize,
		maxPackSize: DefaultMaxPackSize,

		logger: logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/info/refs", h.GetRefs)
	mux.HandleFunc("/git-receive-pack", h.ReceivePack)
	mux.HandleFunc("/git-upload-pack", h.UploadPack)

	h.ServeMux = mux

	for _, opt := range opts {
		opt(h)
	}

	return h
}

func (h *httpGitAPI) AdvertisedReferences(service Service) (*packp.AdvRefs, error) {
	ar := packp.NewAdvRefs()

	if service == Service_ReceivePack {
		if err := ar.Capabilities.Set(capability.DeleteRefs); err != nil {
			return nil, err
		}
		if err := ar.Capabilities.Set(capability.Atomic); err != nil {
			return nil, err
		}
		if err := ar.Capabilities.Set(capability.ReportStatus); err != nil {
			return nil, err
		}
	}

	if service == Service_UploadPack {
		if err := ar.Capabilities.Set(capability.Sideband64k); err != nil {
			return nil, err
		}
		if err := ar.Capabilities.Set(capability.MultiACKDetailed); err != nil {
			return nil, err
		}
	}

	if err := ar.Capabilities.Set(capability.OFSDelta); err != nil {
		return nil, err
	}

	if err := ar.Capabilities.Set(capability.Agent, capability.DefaultAgent); err != nil {
		return nil, err
	}

	refs, err := h.rdb.GetAllRefs()
	if err != nil {
		return nil, err
	}

	head := ""
	var headHash plumbing.Hash

	for name, hex := range refs {
		hash := plumbing.Hash(hex)
		ar.References[name] = hash
		if head == "" || name < head {
			head = name
			headHash = hash
		}
	}

	if head != "" && service == Service_UploadPack {
		ar.Head = &headHash
	}

	return ar, nil
}

func (h *httpGitAPI) GetRefs(w http.ResponseWriter, r *http.Request) {

	svcStr := r.URL.Query().Get("service")
	var service Service
	switch svcStr {
	case "git-upload-pack":
		service = Service_UploadPack
	case "git-receive-pack":
		service = Service_ReceivePack
	default:
		http.Error(w, "unsupported service", http.StatusForbidden)
		return
	}

	w.Header().Set("Content-Type", fmt.Sprintf("application/x-%s-advertisement", svcStr))

	index, err := h.node.ReadIndex(r.Context())
	if err != nil {
		h.logger.Errorf("WaitRead failed, err: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sm := h.rdb.(bdb.Storage)
	if err := sm.WaitForApplyIndex(r.Context(), index); err != nil {
		h.logger.Errorf("WaitForApplyIndex failed, err: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	info, err := h.AdvertisedReferences(service)
	if err != nil {
		h.logger.Errorf("AdvertisedReferences failed, err: %v, service: %v", err, service)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	e := pktline.NewEncoder(w)
	e.EncodeString("# service=" + svcStr + "\n")
	e.Flush()
	info.Encode(w)
}

func (h *httpGitAPI) ReceivePack(w http.ResponseWriter, r *http.Request) {
	req := packp.NewReferenceUpdateRequest()
	var reader io.Reader = r.Body

	sm := h.rdb.(bdb.Storage)
	isLead := sm.GetLeaderTerm() != 0
	if !isLead {
		h.forwardToLeader(r.Context(), w)
		return
	}

	if r.ContentLength == 4 {
		c, err := ioutil.ReadAll(reader)
		if err != nil {
			h.logger.Errorf("ioutil.ReadAll failed, err: %v", err)
			refs.ReportReceivePackError(w, err.Error())
			return
		}
		if string(c) == "0000" {
			return
		}
		reader = bytes.NewReader(c)
	}

	if err := req.Decode(reader); err != nil {
		h.logger.Errorf("req.Decode failed, err: %v", err)
		refs.ReportReceivePackError(w, err.Error())
		return
	}

	var refNames []plumbing.ReferenceName
	for _, cmd := range req.Commands {
		refNames = append(refNames, cmd.Name)
	}

	tx, err := h.node.BeginTx(func(txnLocker raft.MapLocker, storage raft.Storage) (
		map[plumbing.ReferenceName]plumbing.Hash, bool, raft.Unlocker, error) {
		return raft.LockRefList(r.Context(), txnLocker, storage, refNames[0], refNames[1:]...)
	})
	if err != nil {
		h.logger.Warning("begin tx failed, err: ", err)
		refs.ReportReceivePackError(w, err.Error())
		return
	}
	defer tx.Close()

	for _, cmd := range req.Commands {
		if hash := tx.Get(cmd.Name); *hash != cmd.Old {
			refs.ReportReceivePackError(w, "fetch first")
			return
		} else {
			*hash = cmd.New
		}
	}

	content, err := ioutil.ReadAll(req.Packfile)
	if err != nil {
		h.logger.Errorf("ioutil.ReadAll failed, err: %v", err)
		refs.ReportReceivePackError(w, err.Error())
		return
	}

	if len(content) > 0 {
		if err := h.checkLimitSize(content); err != nil {
			h.logger.Errorf("check size failed, err: %v", err)
			refs.ReportReceivePackError(w, err.Error())
			return
		}
	} else {
		content = nil
	}

	writerCh := make(chan io.Writer)
	defer close(writerCh)
	rreq, err := tx.Commit(r.Context(), content, gitexec.WriterCh(writerCh))
	if err != nil {
		h.logger.Errorf("propose failed, err: %v", err)
		refs.ReportReceivePackError(w, err.Error())
		return
	}

	select {
	case <-r.Context().Done():
	case <-rreq.Done():
		if err := rreq.Err(); err != nil {
			refs.ReportReceivePackError(w, err.Error())
		}
	case writerCh <- w:
		<-rreq.Done()
	}

}

func (h *httpGitAPI) UploadPack(w http.ResponseWriter, r *http.Request) {
	h.serviceRPC(w, r, "upload-pack")
}

func (h *httpGitAPI) serviceRPC(w http.ResponseWriter, r *http.Request, service string) {
	defer r.Body.Close()

	actualContentType := r.Header.Get("Content-Type")
	expectContentType := fmt.Sprintf("application/x-git-%s-request", service)
	if actualContentType != expectContentType {
		errorMessage := fmt.Sprintf("Content-Type not support, actualContentType: %v, expectContentType: %v",
			actualContentType, expectContentType)
		h.logger.Error(errorMessage)
		refs.ReportUploadPackError(w, errorMessage)
		return
	}
	w.Header().Set("Content-Type", fmt.Sprintf("application/x-git-%s-result", service))

	var (
		reqBody io.Reader = r.Body
		err     error
	)

	// Handle GZIP
	if r.Header.Get("Content-Encoding") == "gzip" {
		reqBody, err = gzip.NewReader(reqBody)
		if err != nil {
			h.logger.Errorf("gzip.NewReader failed, err: %v", err)
			refs.ReportUploadPackError(w, err.Error())
			return
		}
	}

	var stderr bytes.Buffer

	args := []string{
		service,
		"--stateless-rpc",
		h.path,
	}

	cmd := exec.CommandContext(r.Context(), "git", args...)
	cmd.Stdout = w
	cmd.Stderr = &stderr
	cmd.Stdin = reqBody
	if err := cmd.Run(); err != nil {
		refs.ReportUploadPackError(w, err.Error())
		h.logger.Warning("run cmd failed, args: ", args,
			", err: ", err,
			", stderr: ", stderr.String())
		return
	}
}

func (h *httpGitAPI) forwardToLeader(ctx context.Context, w http.ResponseWriter) {
	status, err := h.node.GetStatus(ctx)
	if err != nil {
		h.logger.Errorf("get status failed, err: %v", err)
		refs.ReportReceivePackError(w, err.Error())
		return
	}

	sm := h.rdb.(bdb.Storage)
	leaderAddrs, err := sm.GetURLsByMemberID(refs.PeerID(status.Lead))
	if err != nil {
		h.logger.Errorf("get leaderAddrs failed, err: %v", err)
		refs.ReportReceivePackError(w, err.Error())
		return
	}

	w.Header().Set("Location", fmt.Sprintf("%s/%s/git-receive-pack",
		leaderAddrs[0], path.Base(h.path)))

	w.WriteHeader(http.StatusFound)
}

func (h *httpGitAPI) checkLimitSize(content []byte) error {
	packSize := len(content)
	if int64(packSize) > h.maxPackSize {
		return fmt.Errorf("pack file size: %d is larger than maxPackSize: %d", packSize, h.maxPackSize)
	}

	r := bytes.NewReader(content)
	scanner := packfile.NewScanner(r)
	_, objectNum, err := scanner.Header()
	if err != nil {
		return err
	}

	for i := uint32(0); i < objectNum; i++ {
		objectHeader, err := scanner.NextObjectHeader()
		if err != nil {
			return err
		}

		if objectHeader.Length > h.maxFileSize {
			return fmt.Errorf("object size: %d larger than %d", objectHeader.Length, h.maxFileSize)
		}
	}

	return nil
}

func (h *httpGitAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	path := r.URL.Path
	method := r.Method

	requestCounter.WithLabelValues(method, path).Inc()
	doingRequest.WithLabelValues(method, path).Inc()
	defer func() {
		doingRequest.WithLabelValues(method, path).Dec()
		requestSeconds.WithLabelValues(method, path).Observe(time.Since(start).Seconds())
	}()

	h.ServeMux.ServeHTTP(w, r)
}
