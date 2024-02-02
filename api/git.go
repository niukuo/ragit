package api

/*
https://github.com/git/git/blob/master/Documentation/technical/http-protocol.txt
*/

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/niukuo/ragit/bdb"
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

	var caps []capability.Capability

	if service == Service_ReceivePack {
		caps = []capability.Capability{
			capability.DeleteRefs,
			capability.Atomic,
			capability.ReportStatus,
			capability.Sideband64k,
			capability.OFSDelta,
		}
	}
	if service == Service_UploadPack {
		caps = []capability.Capability{
			capability.Sideband64k,
			capability.MultiACKDetailed,
			capability.OFSDelta,
		}
	}

	for _, cap := range caps {
		if err := ar.Capabilities.Set(cap); err != nil {
			return nil, err
		}
	}

	if err := ar.Capabilities.Set(capability.Agent, capability.DefaultAgent()); err != nil {
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
