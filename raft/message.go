package raft

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/niukuo/ragit/refs"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type Entry pb.Entry

func (e *Entry) String() string {
	if e == nil {
		return "<nil>"
	}
	content := make(map[string]interface{})
	if e.Term != 0 {
		content["Term"] = e.Term
	}
	if e.Index != 0 {
		content["Index"] = e.Index
	}
	content["Type"] = e.Type
	var dataStr strings.Builder
	for len(e.Data) > 0 {
		var oplog refs.Oplog
		if err := proto.Unmarshal(e.Data, &oplog); err != nil {
			fmt.Fprintf(&dataStr, "(err: %s)", err)
			break
		}
		switch len(oplog.GetOps()) {
		case 0:
			break
		case 1:
			op := oplog.Ops[0]
			fmt.Fprintf(&dataStr, "(1): %s %x..%x",
				op.GetName(), op.OldTarget, op.Target)
		default:
			fmt.Fprintf(&dataStr, "(%d)\n", len(oplog.Ops))
			for _, op := range oplog.Ops {
				fmt.Fprintf(&dataStr, "%s %x..%x\n",
					op.GetName(), op.OldTarget, op.Target)
			}
		}
		break
	}
	if dataStr.Len() > 0 {
		content["data"] = dataStr.String()
	}
	return fmt.Sprintf("%v", content)
}

type Entries []pb.Entry

func (es Entries) String() string {
	switch len(es) {
	case 0:
		return ""
	case 1:
		return (*Entry)(&es[0]).String()
	default:
		var sb strings.Builder
		for i := range es {
			fmt.Fprintf(&sb, "\n%s", (*Entry)(&es[i]))
		}
		return sb.String()
	}
}

type Message pb.Message

func (m *Message) String() string {
	if m == nil {
		return "<nil>"
	}
	content := make(map[string]interface{})
	content["Type"] = m.Type
	content["To"] = PeerID(m.To)
	content["From"] = PeerID(m.From)
	content["Term"] = m.Term
	if m.LogTerm != 0 {
		content["LogTerm"] = m.LogTerm
	}
	if m.Index != 0 {
		content["Index"] = m.Index
	}
	if m.Commit != 0 {
		content["Commit"] = m.Commit
	}
	if cnt := len(m.Entries); cnt > 0 {
		if m.Type != pb.MsgReadIndex && m.Type != pb.MsgReadIndexResp {
			content[fmt.Sprintf("entries(%d)", cnt)] = Entries(m.Entries)
		}
	}
	if !raft.IsEmptySnap(m.Snapshot) {
		content["snapshot"] = (*Snapshot)(&m.Snapshot)
	}
	if m.Reject {
		content["reject"] = m.RejectHint
	}
	if len(m.Context) > 0 {
		content["context"] = m.Context
	}
	return fmt.Sprintf("%v", content)
}

type Snapshot pb.Snapshot

func (m *Snapshot) String() string {
	if m == nil {
		return "<nil>"
	}
	content := make(map[string]interface{})
	content["Index"] = m.Metadata.Index
	content["Term"] = m.Metadata.Term
	peers := make([]PeerID, 0, len(m.Metadata.ConfState.Voters))
	for _, peer := range m.Metadata.ConfState.Voters {
		peers = append(peers, PeerID(peer))
	}
	content["peers"] = peers
	if len(m.Metadata.ConfState.Learners) > 0 {
		learners := make([]PeerID, 0, len(m.Metadata.ConfState.Learners))
		for _, learner := range m.Metadata.ConfState.Learners {
			learners = append(learners, PeerID(learner))
		}
		content["learners"] = learners
	}
	content["data"] = fmt.Sprintf("(%d bytes)", len(m.Data))
	return fmt.Sprintf("%v", content)
}
