package transport

import (
	pb "github.com/m4tth3/raft-grpc-transport/proto"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/types/known/timestamppb"
)

/**
* Protobuf helpers for encoding and decoding Raft RPC messages.
* Contains:
* - Encode/Decode helpers for all Raft RPC messages:
*   - AppendEntriesRequest
*   - AppendEntriesResponse
*   - RequestVoteRequest
*   - RequestVoteResponse
*   - InstallSnapshotRequest
*   - InstallSnapshotResponse
*   - TimeoutNowRequest
*   - TimeoutNowResponse
*   - RequestPreVoteRequest
*   - RequestPreVoteResponse
*/

func encodeAppendEntriesRequest(s *raft.AppendEntriesRequest) *pb.AppendEntriesRequest {
	return &pb.AppendEntriesRequest{
		RpcHeader:         encodeRPCHeader(s.RPCHeader),
		Term:              s.Term,
		Leader:            s.Leader,
		PrevLogEntry:      s.PrevLogEntry,
		PrevLogTerm:       s.PrevLogTerm,
		Entries:           encodeLogs(s.Entries),
		LeaderCommitIndex: s.LeaderCommitIndex,
	}
}

func decodeAppendEntriesRequest(m *pb.AppendEntriesRequest) *raft.AppendEntriesRequest {
	return &raft.AppendEntriesRequest{
		RPCHeader:         decodeRPCHeader(m.RpcHeader),
		Term:              m.Term,
		Leader:            m.Leader,
		PrevLogEntry:      m.PrevLogEntry,
		PrevLogTerm:       m.PrevLogTerm,
		Entries:           decodeLogs(m.Entries),
		LeaderCommitIndex: m.LeaderCommitIndex,
	}
}

func encodeRPCHeader(s raft.RPCHeader) *pb.RPCHeader {
	return &pb.RPCHeader{
		ProtocolVersion: int64(s.ProtocolVersion),
		Id:              s.ID,
		Addr:            s.Addr,
	}
}

func decodeRPCHeader(m *pb.RPCHeader) raft.RPCHeader {
	return raft.RPCHeader{
		ProtocolVersion: raft.ProtocolVersion(m.ProtocolVersion),
		ID:              m.Id,
		Addr:            m.Addr,
	}
}

func encodeLogs(s []*raft.Log) []*pb.Log {
	ret := make([]*pb.Log, len(s))
	for i, l := range s {
		ret[i] = encodeLog(l)
	}
	return ret
}

func decodeLogs(m []*pb.Log) []*raft.Log {
	ret := make([]*raft.Log, len(m))
	for i, l := range m {
		ret[i] = decodeLog(l)
	}
	return ret
}

func encodeLog(s *raft.Log) *pb.Log {
	return &pb.Log{
		Index:      s.Index,
		Term:       s.Term,
		Type:       encodeLogType(s.Type),
		Data:       s.Data,
		Extensions: s.Extensions,
		AppendedAt: timestamppb.New(s.AppendedAt),
	}
}

func decodeLog(m *pb.Log) *raft.Log {
	return &raft.Log{
		Index:      m.Index,
		Term:       m.Term,
		Type:       decodeLogType(m.Type),
		Data:       m.Data,
		Extensions: m.Extensions,
		AppendedAt: m.AppendedAt.AsTime(),
	}
}

func encodeLogType(s raft.LogType) pb.Log_LogType {
	switch s {
	case raft.LogCommand:
		return pb.Log_LOG_COMMAND
	case raft.LogNoop:
		return pb.Log_LOG_NOOP
	case raft.LogAddPeerDeprecated:
		return pb.Log_LOG_ADD_PEER_DEPRECATED
	case raft.LogRemovePeerDeprecated:
		return pb.Log_LOG_REMOVE_PEER_DEPRECATED
	case raft.LogBarrier:
		return pb.Log_LOG_BARRIER
	case raft.LogConfiguration:
		return pb.Log_LOG_CONFIGURATION
	default:
		panic("invalid LogType")
	}
}

func decodeLogType(m pb.Log_LogType) raft.LogType {
	switch m {
	case pb.Log_LOG_COMMAND:
		return raft.LogCommand
	case pb.Log_LOG_NOOP:
		return raft.LogNoop
	case pb.Log_LOG_ADD_PEER_DEPRECATED:
		return raft.LogAddPeerDeprecated
	case pb.Log_LOG_REMOVE_PEER_DEPRECATED:
		return raft.LogRemovePeerDeprecated
	case pb.Log_LOG_BARRIER:
		return raft.LogBarrier
	case pb.Log_LOG_CONFIGURATION:
		return raft.LogConfiguration
	default:
		panic("invalid LogType")
	}
}

func encodeAppendEntriesResponse(s *raft.AppendEntriesResponse) *pb.AppendEntriesResponse {
	return &pb.AppendEntriesResponse{
		RpcHeader:      encodeRPCHeader(s.RPCHeader),
		Term:           s.Term,
		LastLog:        s.LastLog,
		Success:        s.Success,
		NoRetryBackoff: s.NoRetryBackoff,
	}
}

func decodeAppendEntriesResponse(m *pb.AppendEntriesResponse) *raft.AppendEntriesResponse {
	return &raft.AppendEntriesResponse{
		RPCHeader:      decodeRPCHeader(m.RpcHeader),
		Term:           m.Term,
		LastLog:        m.LastLog,
		Success:        m.Success,
		NoRetryBackoff: m.NoRetryBackoff,
	}
}

func encodeRequestVoteRequest(s *raft.RequestVoteRequest) *pb.RequestVoteRequest {
	return &pb.RequestVoteRequest{
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		Term:               s.Term,
		Candidate:          s.Candidate,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		LeadershipTransfer: s.LeadershipTransfer,
	}
}

func decodeRequestVoteRequest(m *pb.RequestVoteRequest) *raft.RequestVoteRequest {
	return &raft.RequestVoteRequest{
		RPCHeader:          decodeRPCHeader(m.RpcHeader),
		Term:               m.Term,
		Candidate:          m.Candidate,
		LastLogIndex:       m.LastLogIndex,
		LastLogTerm:        m.LastLogTerm,
		LeadershipTransfer: m.LeadershipTransfer,
	}
}

func encodeRequestVoteResponse(s *raft.RequestVoteResponse) *pb.RequestVoteResponse {
	return &pb.RequestVoteResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Peers:     s.Peers,
		Granted:   s.Granted,
	}
}

func decodeRequestVoteResponse(m *pb.RequestVoteResponse) *raft.RequestVoteResponse {
	return &raft.RequestVoteResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
		Term:      m.Term,
		Peers:     m.Peers,
		Granted:   m.Granted,
	}
}

func encodeInstallSnapshotRequest(s *raft.InstallSnapshotRequest) *pb.InstallSnapshotRequest {
	return &pb.InstallSnapshotRequest{
		RpcHeader:          encodeRPCHeader(s.RPCHeader),
		SnapshotVersion:    int64(s.SnapshotVersion),
		Term:               s.Term,
		Leader:             s.Leader,
		LastLogIndex:       s.LastLogIndex,
		LastLogTerm:        s.LastLogTerm,
		Peers:              s.Peers,
		Configuration:      s.Configuration,
		ConfigurationIndex: s.ConfigurationIndex,
		Size:               s.Size,
	}
}

func decodeInstallSnapshotRequest(m *pb.InstallSnapshotRequest) *raft.InstallSnapshotRequest {
	return &raft.InstallSnapshotRequest{
		RPCHeader:          decodeRPCHeader(m.RpcHeader),
		SnapshotVersion:    raft.SnapshotVersion(m.SnapshotVersion),
		Term:               m.Term,
		Leader:             m.Leader,
		LastLogIndex:       m.LastLogIndex,
		LastLogTerm:        m.LastLogTerm,
		Peers:              m.Peers,
		Configuration:      m.Configuration,
		ConfigurationIndex: m.ConfigurationIndex,
		Size:               m.Size,
	}
}

func encodeInstallSnapshotResponse(s *raft.InstallSnapshotResponse) *pb.InstallSnapshotResponse {
	return &pb.InstallSnapshotResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Success:   s.Success,
	}
}

func decodeInstallSnapshotResponse(m *pb.InstallSnapshotResponse) *raft.InstallSnapshotResponse {
	return &raft.InstallSnapshotResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
		Term:      m.Term,
		Success:   m.Success,
	}
}

func encodeTimeoutNowRequest(s *raft.TimeoutNowRequest) *pb.TimeoutNowRequest {
	return &pb.TimeoutNowRequest{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}

func decodeTimeoutNowRequest(m *pb.TimeoutNowRequest) *raft.TimeoutNowRequest {
	return &raft.TimeoutNowRequest{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
	}
}

func encodeTimeoutNowResponse(s *raft.TimeoutNowResponse) *pb.TimeoutNowResponse {
	return &pb.TimeoutNowResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
	}
}

func decodeTimeoutNowResponse(m *pb.TimeoutNowResponse) *raft.TimeoutNowResponse {
	return &raft.TimeoutNowResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
	}
}

func encodeRequestPreVoteRequest(s *raft.RequestPreVoteRequest) *pb.RequestPreVoteRequest {
	return &pb.RequestPreVoteRequest{
		RpcHeader:    encodeRPCHeader(s.RPCHeader),
		Term:         s.Term,
		LastLogIndex: s.LastLogIndex,
		LastLogTerm:  s.LastLogTerm,
	}
}

func decodeRequestPreVoteRequest(m *pb.RequestPreVoteRequest) *raft.RequestPreVoteRequest {
	return &raft.RequestPreVoteRequest{
		RPCHeader:    decodeRPCHeader(m.RpcHeader),
		Term:         m.Term,
		LastLogIndex: m.LastLogIndex,
		LastLogTerm:  m.LastLogTerm,
	}
}

func encodeRequestPreVoteResponse(s *raft.RequestPreVoteResponse) *pb.RequestPreVoteResponse {
	return &pb.RequestPreVoteResponse{
		RpcHeader: encodeRPCHeader(s.RPCHeader),
		Term:      s.Term,
		Granted:   s.Granted,
	}
}

func decodeRequestPreVoteResponse(m *pb.RequestPreVoteResponse) *raft.RequestPreVoteResponse {
	return &raft.RequestPreVoteResponse{
		RPCHeader: decodeRPCHeader(m.RpcHeader),
		Term:      m.Term,
		Granted:   m.Granted,
	}
}
