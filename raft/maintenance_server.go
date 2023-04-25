package raft

import (
	"context"
	"errors"
	"time"

	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	serverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

var (
	ErrNotImplemented        = errors.New("Not Implemented")
	ErrTimeoutLeaderTransfer = errors.New("request timed out, leader transfer took too long")
)

var _ serverpb.MaintenanceServer = (*maintenanceServer)(nil)

type MaintenanceServer = *maintenanceServer

type maintenanceServer struct {
	c *ServerConfig

	eventLogger logging.Logger
}

func NewMaintenanceServer(config ServerConfig) MaintenanceServer {
	return &maintenanceServer{
		c: &config,

		eventLogger: logging.GetLogger("event.maintenance_server"),
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
	ctx context.Context, request *serverpb.MoveLeaderRequest) (*serverpb.MoveLeaderResponse, error) {

	if err := ms.c.raft.withPipeline(ctx, func(node *raft.RawNode) error {

		s := Status(node.Status())

		if ms.c.id != refs.PeerID(s.Lead) {
			return rpctypes.ErrNotLeader
		}

		if transferee, ok := s.Progress[request.TargetID]; !ok || transferee.IsLearner {
			return rpctypes.ErrBadLeaderTransferee
		}

		msg := pb.Message{Type: pb.MsgTransferLeader, From: request.TargetID, To: s.Lead}
		if err := node.Step(msg); err != nil {
			return err
		}

		return nil

	}); err != nil {
		return nil, err
	}

	for ms.c.storage.GetLeaderTerm() != 0 {
		select {
		case <-ctx.Done():
			return nil, ErrTimeoutLeaderTransfer
		case <-time.After(1 * time.Second):
		}
	}

	ms.eventLogger.Infof("move leader %d to %d", ms.c.id, request.TargetID)

	return &serverpb.MoveLeaderResponse{}, nil
}

func (ms *maintenanceServer) Downgrade(
	context.Context, *serverpb.DowngradeRequest) (*serverpb.DowngradeResponse, error) {
	return nil, ErrNotImplemented
}
