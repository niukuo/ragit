package raft

import (
	"context"
	"errors"

	serverpb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/raft"
)

var ErrNotImplemented = errors.New("Not Implemented")

var _ serverpb.MaintenanceServer = (*maintenanceServer)(nil)

type MaintenanceServer = *maintenanceServer

type maintenanceServer struct {
	c *ServerConfig
}

func NewMaintenanceServer(config ServerConfig) MaintenanceServer {
	return &maintenanceServer{
		c: &config,
	}
}

func (ms *maintenanceServer) Status(
	ctx context.Context,
	request *serverpb.StatusRequest,
) (*serverpb.StatusResponse, error) {

	var s Status

	if err := ms.c.raft.withPipeline(ctx, func(node *raft.RawNode) error {

		s = Status(node.Status())

		return nil

	}); err != nil {
		return nil, err
	}

	isLearner := false

	if _, ok := s.Config.Learners[uint64(ms.c.id)]; ok {
		isLearner = true
	}

	header := &serverpb.ResponseHeader{
		ClusterId: uint64(ms.c.clusterId),
		MemberId:  uint64(ms.c.id),
		RaftTerm:  s.Term,
	}

	resp := &serverpb.StatusResponse{
		Header:           header,
		Leader:           s.Lead,
		RaftIndex:        s.Commit,
		RaftAppliedIndex: s.Applied,
		RaftTerm:         s.Term,
		IsLearner:        isLearner,
	}

	if resp.Leader == raft.None {
		resp.Errors = append(resp.Errors, "no leader")
	}

	return resp, nil
}

func (ms *maintenanceServer) Alarm(
	context.Context, *serverpb.AlarmRequest) (*serverpb.AlarmResponse, error) {
	return nil, ErrNotImplemented
}

func (ms *maintenanceServer) Defragment(
	context.Context, *serverpb.DefragmentRequest) (*serverpb.DefragmentResponse, error) {
	return nil, ErrNotImplemented
}

func (ms *maintenanceServer) Hash(
	context.Context, *serverpb.HashRequest) (*serverpb.HashResponse, error) {
	return nil, ErrNotImplemented
}

func (ms *maintenanceServer) HashKV(
	context.Context, *serverpb.HashKVRequest) (*serverpb.HashKVResponse, error) {
	return nil, ErrNotImplemented
}

func (ms *maintenanceServer) Snapshot(
	*serverpb.SnapshotRequest, serverpb.Maintenance_SnapshotServer) error {
	return ErrNotImplemented
}

func (ms *maintenanceServer) MoveLeader(
	context.Context, *serverpb.MoveLeaderRequest) (*serverpb.MoveLeaderResponse, error) {
	return nil, ErrNotImplemented
}
