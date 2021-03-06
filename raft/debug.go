package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/raft"
	tracker "go.etcd.io/etcd/raft"
)

func (rc *raftNode) getWAL(w http.ResponseWriter, r *http.Request) {
	var first uint64 = 1
	var count uint64 = 100
	var maxSize uint64 = 1048576
	if v := r.FormValue("first"); v != "" {
		num, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		first = num
	}

	if v := r.FormValue("count"); v != "" {
		num, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		count = num
	}

	if v := r.FormValue("max_size"); v != "" {
		num, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		maxSize = num
	}

	entries, err := rc.storage.Entries(first, first+count, maxSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmtOp := func(op *refs.Oplog_Op) string {
		return fmt.Sprintf("%s %x..%x", op.GetName(), op.OldTarget, op.Target)
	}
	str := raft.DescribeEntries(entries, func(b []byte) (str string) {

		if len(b) == 0 {
			return "<empty>"
		}

		var oplog refs.Oplog
		if err := proto.Unmarshal(b, &oplog); err != nil {
			if len(b) > 256 {
				return string(b[:251]) +
					fmt.Sprintf("...(%d bytes)", len(b))
			}
			return string(b)
		}

		defer func() {
			str += fmt.Sprintf(", data: %d bytes", len(oplog.ObjPack))
		}()

		switch cnt := len(oplog.Ops); cnt {
		case 0:
			return "ops(0): []"
		case 1:
			return "ops(1): " + fmtOp(oplog.Ops[0])
		default:
			var sb strings.Builder
			fmt.Fprintf(&sb, "ops(%d): [\n", cnt)
			for _, op := range oplog.Ops {
				sb.WriteString(fmtOp(op))
				sb.WriteByte('\n')
			}
			sb.WriteByte(']')
			return sb.String()
		}
	})

	fmt.Fprint(w, str)
}

func (rc *raftNode) getServerStat(w http.ResponseWriter, r *http.Request) {
	stat := json.RawMessage(rc.serverStats.JSON())
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	encoder.Encode(stat)

}

func (rc *raftNode) getLeaderStat(w http.ResponseWriter, r *http.Request) {
	var stat map[string]json.RawMessage
	if err := json.Unmarshal(rc.leaderStats.JSON(), &stat); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var followers map[string]map[string]json.RawMessage
	if err := json.Unmarshal(stat["followers"], &followers); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	newFollowers := make([]map[string]json.RawMessage, 0, len(followers))
	for str, follower := range followers {
		convStr := str
		id, err := strconv.ParseUint(str, 16, 64)
		if err == nil {
			convStr = refs.PeerID(id).String()
		}
		follower["id"] = json.RawMessage(strconv.Quote(convStr))
		newFollowers = append(newFollowers, follower)

		sort.Slice(newFollowers, func(i, j int) bool {
			return string(newFollowers[i]["id"]) < string(newFollowers[j]["id"])
		})
	}

	fdata, err := json.Marshal(newFollowers)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	stat["followers"] = json.RawMessage(fdata)

	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	encoder.Encode(stat)
}

func (rc *raftNode) getStatus(w http.ResponseWriter, r *http.Request) {

	status := (*raft.SoftState)(atomic.LoadPointer(&rc.softState))
	if err := rc.raftResult; err != nil {
		fmt.Fprintln(w, "state: err,", err)
	} else if status != nil {
		fmt.Fprintln(w, "state:", status.RaftState)
		fmt.Fprintln(w, "leader:", PeerID(status.Lead))
	} else {
		fmt.Fprintln(w, "state: init")
	}
	rc.storage.Describe(w)
	rc.executor.Describe(w)
	if status == nil || status.RaftState != raft.StateLeader {
		return
	}

	type ReplicaStatus struct {
		id  PeerID
		typ raft.ProgressType
		pr  tracker.Progress
	}
	replicators := make([]ReplicaStatus, 0)

	ctx, cancel := context.WithTimeout(r.Context(), 100*time.Millisecond)
	defer cancel()
	if err := rc.withPipeline(ctx, func(node *raft.RawNode) error {
		node.WithProgress(func(id uint64, typ raft.ProgressType, pr tracker.Progress) {
			replicators = append(replicators, ReplicaStatus{
				id:  PeerID(id),
				typ: typ,
				pr:  pr,
			})
		})
		return nil
	}); err != nil {
		fmt.Fprintln(w, "progress: err, ", err)
		return
	}

	sort.Slice(replicators, func(i, j int) bool {
		return replicators[i].id < replicators[j].id
	})

	for _, rep := range replicators {
		fmt.Fprintf(w, "replicator@%s: ", rep.id)
		if rep.typ != raft.ProgressTypePeer {
			fmt.Fprintf(w, "%v ", rep.typ)
		}
		switch state := rep.pr.State; state {
		case tracker.ProgressStateProbe:
			state := "probe"
			if rep.pr.Paused {
				state = "paused"
			}
			if rep.pr.RecentActive {
				state += ",active"
			}
			fmt.Fprintf(w, "%s ", state)
			fallthrough
		case tracker.ProgressStateReplicate:
			fmt.Fprintf(w, "match=%v, next=%v",
				rep.pr.Match, rep.pr.Next)
		case tracker.ProgressStateSnapshot:
			fmt.Fprintf(w, "snapshot=%v", rep.pr.PendingSnapshot)
		default:
			fmt.Fprint(w, state)
		}
		fmt.Fprintln(w)
	}
}
