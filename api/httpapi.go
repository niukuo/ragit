// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/raft/raftpb"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	storage     Storage
	node        raft.Node
	confChangeC chan<- raftpb.ConfChange
}

func (h *httpKVAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		v, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on PUT (%v)\n", err)
			http.Error(w, "Failed on PUT", http.StatusBadRequest)
			return
		}

		lines := strings.Split(strings.TrimSpace(string(v)), "\n")

		oplog := refs.Oplog{
			Ops: make([]*refs.Oplog_Op, 0, len(lines)),
		}
		for _, line := range lines {
			slices := strings.SplitN(line, " ", 4)
			if len(slices) != 3 {
				http.Error(w, "invalid line: "+line, http.StatusBadRequest)
				return
			}

			oldTarget, err := hex.DecodeString(slices[0])
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			target, err := hex.DecodeString(slices[1])
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			op := &refs.Oplog_Op{
				Name:      proto.String(slices[2]),
				OldTarget: oldTarget,
				Target:    target,
			}

			oplog.Ops = append(oplog.Ops, op)
		}

		req, err := h.node.Propose(r.Context(), oplog, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-req.Done():
			if err := req.Err(); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}

	case http.MethodGet:
		snap, err := h.storage.Snapshot()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		io.WriteString(w, string(snap.Data))

	case http.MethodPost:
		ccVal, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Failed to read on POST (%v)\n", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var ccParams raft.ConfChangeParams
		err = json.Unmarshal(ccVal, &ccParams)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		now := time.Now()
		memberID := refs.ComputePeerID(ccParams.PeerUrls, &now)

		var typ raftpb.ConfChangeType
		switch action := r.URL.Query().Get("action"); action {
		case "add":
			typ = raftpb.ConfChangeAddNode
		case "add_learner":
			typ = raftpb.ConfChangeAddLearnerNode
		case "remove":
			typ = raftpb.ConfChangeRemoveNode
			id := strings.TrimPrefix(r.URL.Path, "/raft/members/")
			val, err := strconv.ParseUint(id, 16, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			memberID = refs.PeerID(val)
		default:
			http.Error(w, "invalid action: "+action, http.StatusBadRequest)
			return
		}

		cc := raftpb.ConfChange{
			Type:   typ,
			NodeID: uint64(memberID),
		}

		if typ != raftpb.ConfChangeRemoveNode {
			member := refs.NewMember(memberID, ccParams.PeerUrls)
			mb, err := json.Marshal(member)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			cc.Context = mb
		}

		h.confChangeC <- cc

		// As above, optimistic that raft will apply the conf change
		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", http.MethodPut)
		w.Header().Add("Allow", http.MethodGet)
		w.Header().Add("Allow", http.MethodPost)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// serveHttpKVAPI starts a key-value server with a GET/PUT API and listens.
func NewHandler(db Storage, raft raft.Node) http.Handler {
	return &httpKVAPI{
		storage: db,
		node:    raft,
	}
}
