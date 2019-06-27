package raft

import (
	"fmt"

	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

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
	if len(m.Entries) > 0 {
		content["entries"] = m.Entries
	}
	if !raft.IsEmptySnap(m.Snapshot) {
		content["snapshot"] = m.Snapshot
	}
	if m.Reject {
		content["reject"] = m.RejectHint
	}
	if len(m.Context) > 0 {
		content["context"] = m.Context
	}
	return fmt.Sprintf("%v", content)
}
