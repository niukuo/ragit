package raft

import (
	"fmt"

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
	dataStr := ""
	data := e.Data
	maxLen := 128
	if len(data) > maxLen {
		data = data[:maxLen]
		dataStr = fmt.Sprintf("(%d/%d bytes)", len(data), len(e.Data)) + string(data)
	} else {
		dataStr = fmt.Sprintf("(%d bytes)", len(e.Data)) + string(data)
	}
	content["data"] = dataStr
	return fmt.Sprintf("%v", content)
}

type Entries []pb.Entry

func (es Entries) String() string {
	s := make([]*Entry, 0, len(es))
	for i := range es {
		s = append(s, (*Entry)(&es[i]))
	}
	return fmt.Sprintf("%v", s)
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
	if len(m.Entries) > 0 {
		content["entries"] = Entries(m.Entries)
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
