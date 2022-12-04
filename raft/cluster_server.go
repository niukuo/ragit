package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/niukuo/ragit/logging"
	"github.com/niukuo/ragit/refs"
	serverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
)

var _ serverpb.ClusterServer = (*clusterServer)(nil)

const (
	clusterPrefix = "/etcdserverpb.Cluster/"
)

type ClusterServer = *clusterServer

type clusterServer struct {
	*ServerConfig

	eventLogger logging.Logger
}

type ServerConfig struct {
	clusterId types.ID
	id        PeerID

	storage Storage
	raft    Raft
	channel RpcChannel

	newMemberID func(addr []string) PeerID
}

func NewClusterServer(config ServerConfig) ClusterServer {
	return &clusterServer{
		ServerConfig: &config,

		eventLogger: logging.GetLogger("event.cluster_server"),
	}
}

type ConfChangeContext struct {
	refs.Member
	IsPromote bool
}

func (cs *clusterServer) MemberAdd(
	ctx context.Context,
	request *serverpb.MemberAddRequest,
) (*serverpb.MemberAddResponse, error) {

	if _, err := types.NewURLs(request.PeerURLs); err != nil {
		return nil, err
	}

	id := cs.newMemberID(request.PeerURLs)

	confChangeContext := ConfChangeContext{
		Member:    refs.NewMember(id, request.PeerURLs),
		IsPromote: false,
	}

	data, err := json.Marshal(confChangeContext)
	if err != nil {
		return nil, err
	}

	cc := pb.ConfChange{
		Type:    pb.ConfChangeAddNode,
		NodeID:  uint64(id),
		Context: data,
	}

	if request.IsLearner {
		cc.Type = pb.ConfChangeAddLearnerNode
	}

	if err := cs.proposeConfChange(ctx, cc); err != nil {
		return nil, err
	}

	return &serverpb.MemberAddResponse{
		Header: cs.header(ctx),
		Member: &serverpb.Member{
			ID:        uint64(id),
			PeerURLs:  request.PeerURLs,
			IsLearner: request.IsLearner,
		},
	}, nil
}

func (cs *clusterServer) MemberRemove(
	ctx context.Context,
	request *serverpb.MemberRemoveRequest,
) (*serverpb.MemberRemoveResponse, error) {

	cc := pb.ConfChange{
		Type:   pb.ConfChangeRemoveNode,
		NodeID: uint64(request.ID),
	}

	if err := cs.proposeConfChange(ctx, cc); err != nil {
		return nil, err
	}

	return &serverpb.MemberRemoveResponse{
		Header: cs.header(ctx),
	}, nil
}

func (cs *clusterServer) MemberPromote(
	ctx context.Context,
	request *serverpb.MemberPromoteRequest,
) (*serverpb.MemberPromoteResponse, error) {

	confChangeContext := ConfChangeContext{
		Member:    refs.NewMember(PeerID(request.ID), nil),
		IsPromote: true,
	}

	data, err := json.Marshal(confChangeContext)
	if err != nil {
		return nil, err
	}

	cc := pb.ConfChange{
		Type:    pb.ConfChangeAddNode,
		NodeID:  uint64(request.ID),
		Context: data,
	}

	if err := cs.proposeConfChange(ctx, cc); err != nil {
		return nil, err
	}

	return &serverpb.MemberPromoteResponse{
		Header: cs.header(ctx),
	}, nil
}

func (cs *clusterServer) MemberUpdate(
	ctx context.Context,
	request *serverpb.MemberUpdateRequest,
) (*serverpb.MemberUpdateResponse, error) {

	if _, err := types.NewURLs(request.PeerURLs); err != nil {
		return nil, err
	}

	id := PeerID(request.ID)

	confChangeContext := ConfChangeContext{
		Member:    refs.NewMember(id, request.PeerURLs),
		IsPromote: false,
	}

	data, err := json.Marshal(confChangeContext)
	if err != nil {
		return nil, err
	}

	cc := pb.ConfChange{
		Type:    pb.ConfChangeUpdateNode,
		NodeID:  uint64(id),
		Context: data,
	}

	if err := cs.proposeConfChange(ctx, cc); err != nil {
		return nil, err
	}

	return &serverpb.MemberUpdateResponse{
		Header: cs.header(ctx),
	}, nil

}

func (cs *clusterServer) MemberList(
	ctx context.Context,
	request *serverpb.MemberListRequest,
) (*serverpb.MemberListResponse, error) {

	state, err := cs.storage.GetInitState()
	if err != nil {
		return nil, err
	}

	learners := make(map[PeerID]bool, len(state.ConfState.Learners))
	for _, id := range state.ConfState.Learners {
		learners[PeerID(id)] = true
	}

	pbMembers := make([]*serverpb.Member, 0, len(state.Members))
	for _, member := range state.Members {
		pbMembers = append(pbMembers, &serverpb.Member{
			ID:         uint64(member.ID),
			Name:       member.ID.Format(),
			PeerURLs:   member.PeerURLs,
			ClientURLs: member.PeerURLs,
			IsLearner:  learners[member.ID],
		})
	}

	return &serverpb.MemberListResponse{
		Header: &serverpb.ResponseHeader{
			ClusterId: uint64(cs.clusterId),
			MemberId:  uint64(cs.id),
		},
		Members: pbMembers,
	}, nil
}

func (cs *clusterServer) proposeConfChange(ctx context.Context, cc pb.ConfChange) error {

	term := cs.storage.GetLeaderTerm()
	if term == 0 {
		return errors.New("not leader")
	}

	ctx = WithExpectedTerm(ctx, term)

	if err := cs.raft.proposeConfChange(ctx, cc); err != nil {
		cs.eventLogger.Warning("apply conf change",
			", confChange: ", cc.String(),
			", err: ", err,
		)
		return err
	}

	cs.eventLogger.Info("apply conf change",
		", op: ", cc.Type,
		", id: ", types.ID(cc.NodeID),
		", term: ", term,
		", ctx: ", string(cc.Context),
	)

	return nil
}

func (cs *clusterServer) header(ctx context.Context) *serverpb.ResponseHeader {

	var term uint64
	if v := ctx.Value(CtxExpectedTermKey); v != nil {
		term = v.(uint64)
	}

	return &serverpb.ResponseHeader{
		ClusterId: uint64(cs.clusterId),
		MemberId:  uint64(cs.id),
		RaftTerm:  term,
	}
}

func (cs *clusterServer) forwardToLeader(ctx context.Context, method string, req interface{}) (interface{}, error) {

	var resp interface{}

	switch strings.TrimPrefix(method, clusterPrefix) {
	case "MemberList":
		resp = new(serverpb.MemberListResponse)
	case "MemberAdd":
		resp = new(serverpb.MemberAddResponse)
	case "MemberRemove":
		resp = new(serverpb.MemberRemoveResponse)
	case "MemberPromote":
		resp = new(serverpb.MemberPromoteResponse)
	case "MemberUpdate":
		resp = new(serverpb.MemberUpdateResponse)
	}

	if err := cs.channel.Invoke(ctx, method, req, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func NewMemberUnaryInterceptor(cs ClusterServer) grpc.UnaryServerInterceptor {

	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {

		if strings.HasPrefix(info.FullMethod, clusterPrefix) {

			var s Status
			if err := cs.raft.withPipeline(ctx, func(node *raft.RawNode) error {
				s = Status(node.Status())
				return nil

			}); err != nil {
				return nil, err
			}

			switch s.RaftState {
			case raft.StateLeader:
				return handler(ctx, req)
			case raft.StateFollower:
				return cs.forwardToLeader(ctx, info.FullMethod, req)
			default:
				return nil, fmt.Errorf("cannot handle, state: %s", s.RaftState)
			}
		}

		return handler(ctx, req)
	}
}
