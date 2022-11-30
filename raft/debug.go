package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

func (rc *readyHandler) getWAL(w http.ResponseWriter, r *http.Request) {
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

func (rc *readyHandler) getServerStat(w http.ResponseWriter, r *http.Request) {
	stat := json.RawMessage(rc.transport.ServerStats.JSON())
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	encoder.Encode(stat)

}

func (rc *readyHandler) getLeaderStat(w http.ResponseWriter, r *http.Request) {
	stat := json.RawMessage(rc.transport.LeaderStats.JSON())
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	encoder.SetEscapeHTML(false)
	encoder.Encode(stat)
}

func (rc *readyHandler) getForwardStat(w http.ResponseWriter, r *http.Request) {
	rc.channel.Describe(w)
}

func (rc *readyHandler) getStatus(w http.ResponseWriter, r *http.Request) {
	rc.raft.Describe(w)

	// TODO: atomic.LoadPointer
	if doingReadState := rc.readIndexDoing; doingReadState != nil {
		fmt.Fprintf(w, "read_index_doing_id: %x\n", doingReadState.id)
		fmt.Fprintln(w, "read_index_doing_term: ", doingReadState.term)
	}

	rc.storage.Describe(w)
}

func (rc *readyHandler) getReadIndex(w http.ResponseWriter, r *http.Request) {
	state := rc.proposeReadIndex()

	ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		fmt.Fprint(w, ctx.Err())
		return
	case <-rc.Runner.Done():
		fmt.Fprintf(w, "err: %s\n", ErrStopped)
		return
	case <-state.proposed:
		fmt.Fprintf(w, "ctx: %x\n", state.id)
		fmt.Fprintf(w, "term: %d\n", state.term)
	}

	ctx, cancel = context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		fmt.Fprint(w, ctx.Err())
	case <-state.done:
		if err := state.err; err != nil {
			fmt.Fprint(w, err)
		} else {
			fmt.Fprintf(w, "index: %d\n", state.index)
		}
	case <-rc.Runner.Done():
		fmt.Fprint(w, ErrStopped)
	}

}

func (rc *raftNode) Describe(w io.Writer) {
	status := (*raft.SoftState)(atomic.LoadPointer(&rc.softState))

	select {
	case <-rc.Runner.Done():
		err := rc.Runner.Error()
		fmt.Fprintln(w, "state: stopped, err:", err)
	default:
		if status != nil {
			fmt.Fprintln(w, "state:", status.RaftState)
			fmt.Fprintln(w, "leader:", PeerID(status.Lead))
		} else {
			fmt.Fprintln(w, "state: init")
		}
	}

	rc.requests.Describe(w)

	if status == nil || status.RaftState != raft.StateLeader {
		return
	}

	type ReplicaStatus struct {
		id  PeerID
		typ raft.ProgressType
		pr  tracker.Progress
	}
	replicators := make([]ReplicaStatus, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
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
		switch rep.typ {
		case raft.ProgressTypePeer:
			fmt.Fprintf(w, "replicator@%s: ", rep.id)
		case raft.ProgressTypeLearner:
			fmt.Fprintf(w, "learner@%s: ", rep.id)
		default:
			fmt.Fprintf(w, "%v@%s: ", rep.typ, rep.id)
		}

		switch state := rep.pr.State; state {
		case tracker.StateProbe:
			state := "probe"
			if rep.pr.ProbeSent {
				state = "paused"
			}
			if rep.pr.RecentActive {
				state += ",active"
			}
			fmt.Fprintf(w, "%s ", state)
			fallthrough
		case tracker.StateReplicate:
			fmt.Fprintf(w, "match=%v, next=%v",
				rep.pr.Match, rep.pr.Next)
		case tracker.StateSnapshot:
			fmt.Fprintf(w, "snapshot=%v", rep.pr.PendingSnapshot)
		default:
			fmt.Fprint(w, state)
		}
		fmt.Fprintln(w)
	}
}
