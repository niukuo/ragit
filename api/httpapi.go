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
	"bytes"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/niukuo/ragit/raft"
)

// Handler for a http based key-value store backed by raft
type httpKVAPI struct {
	storage Storage
	node    raft.Node
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

		tx, err := h.node.BeginTx(func(txnLocker raft.MapLocker, storage raft.Storage) (map[plumbing.ReferenceName]plumbing.Hash, bool, raft.Unlocker, error) {
			return raft.LockGlobal(r.Context(), txnLocker, nil, storage)
		})
		if err != nil {
			log.Printf("begin tx failed, err: %s", err)
			http.Error(w, "begin tx failed: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer tx.Close()

		lines := strings.Split(strings.TrimSpace(string(v)), "\n")

		for _, line := range lines {
			slices := strings.SplitN(line, " ", 4)
			if len(slices) != 3 {
				http.Error(w, "invalid line: "+line, http.StatusBadRequest)
				return
			}

			refName := slices[2]

			oldTarget, err := hex.DecodeString(slices[0])
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if hash := tx.Get(plumbing.ReferenceName(refName)); !bytes.Equal(hash[:], oldTarget) {
				http.Error(w, "not match: "+refName, http.StatusBadRequest)
				return
			} else {
				target, err := hex.DecodeString(slices[1])
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				} else if l := len(target); l != len(plumbing.Hash{}) {
					http.Error(w, "invalid target: "+refName, http.StatusBadRequest)
					return
				} else {
					copy(hash[:], target)
				}
			}

		}

		req, err := tx.Commit(r.Context(), nil, nil)
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

	default:
		w.Header().Set("Allow", http.MethodPut)
		w.Header().Add("Allow", http.MethodGet)
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
