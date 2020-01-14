package api

/*
https://github.com/git/git/blob/master/Documentation/technical/http-protocol.txt
*/

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os/exec"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/format/pktline"
	"gopkg.in/src-d/go-git.v4/plumbing/protocol/packp"
	"gopkg.in/src-d/go-git.v4/plumbing/protocol/packp/capability"
)

type Service int

const (
	Service_Invalid Service = iota
	Service_UploadPack
	Service_ReceivePack
)

type httpGitAPI struct {
	path string
	rdb  Storage
	node raft.Node
	*http.ServeMux

	logger logging.Logger
}

func NewGitHandler(
	path string,
	rdb Storage,
	node raft.Node,
	logger logging.Logger,
) *httpGitAPI {
	h := &httpGitAPI{
		path:   path,
		rdb:    rdb,
		node:   node,
		logger: logger,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/info/refs", h.GetRefs)
	mux.HandleFunc("/git-receive-pack", h.ReceivePack)
	mux.HandleFunc("/git-upload-pack", h.UploadPack)

	h.ServeMux = mux

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

func packetWrite(str string) []byte {
	s := strconv.FormatInt(int64(len(str)+4), 16)
	if len(s)%4 != 0 {
		s = strings.Repeat("0", 4-len(s)%4) + s
	}
	return []byte(s + str)
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

	info, err := h.AdvertisedReferences(service)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	e := pktline.NewEncoder(w)
	e.EncodeString("# service=" + svcStr + "\n")
	e.Flush()
	info.Encode(w)
}

func reportError(w http.ResponseWriter, err error, statusCode int) {
	w.WriteHeader(statusCode)
	refs.ReportError(w, err)
}

func (h *httpGitAPI) ReceivePack(w http.ResponseWriter, r *http.Request) {

	req := packp.NewReferenceUpdateRequest()
	var reader io.Reader = r.Body

	if r.ContentLength == 4 {
		c, err := ioutil.ReadAll(reader)
		if err != nil {
			reportError(w, err, http.StatusBadRequest)
			return
		}
		if string(c) == "0000" {
			return
		}
		reader = bytes.NewReader(c)
	}

	if err := req.Decode(reader); err != nil {
		reportError(w, err, http.StatusBadRequest)
		return
	}

	content, err := ioutil.ReadAll(req.Packfile)
	if err != nil {
		reportError(w, err, http.StatusInternalServerError)
		return
	}

	oplog := refs.Oplog{
		Ops:     make([]*refs.Oplog_Op, 0, len(req.Commands)),
		ObjPack: content,
	}

	for _, cmd := range req.Commands {
		op := &refs.Oplog_Op{
			Name: proto.String(cmd.Name.String()),
		}
		if !cmd.New.IsZero() {
			op.Target = cmd.New[:]
		}
		if !cmd.Old.IsZero() {
			op.OldTarget = cmd.Old[:]
		}
		oplog.Ops = append(oplog.Ops, op)
	}

	if err := h.node.Propose(raft.WithResponseWriter(r.Context(), w), oplog); err != nil {
		reportError(w, err, http.StatusBadRequest)
		return
	}

}

func (h *httpGitAPI) UploadPack(w http.ResponseWriter, r *http.Request) {
	h.serviceRPC(w, r, "upload-pack")
}

func (h *httpGitAPI) serviceRPC(w http.ResponseWriter, r *http.Request, service string) {
	defer r.Body.Close()

	if r.Header.Get("Content-Type") != fmt.Sprintf("application/x-git-%s-request", service) {
		w.WriteHeader(http.StatusUnauthorized)
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
			http.Error(w, err.Error(), http.StatusInternalServerError)
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
		http.Error(w, err.Error(), http.StatusInternalServerError)
		h.logger.Warning("run cmd failed, args: ", args,
			", err: ", err,
			", stderr: ", stderr.String())
		return
	}
}
