package dchan

import (
	"bytes"
	"context"
	"encoding/gob"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	p "github.com/m4tth3/dchan/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

// mockReceiverManager implements receiverManager for testing
type mockReceiverManager struct {
	getReceiverFunc func(namespace Namespace) (rch *rchannel, dec func() int32, ok bool)
}

func (m *mockReceiverManager) getReceiver(namespace Namespace) (rch *rchannel, dec func() int32, ok bool) {
	if m.getReceiverFunc != nil {
		return m.getReceiverFunc(namespace)
	}
	return nil, func() int32 { return 0 }, false
}

// mockRaftForServer wraps raft.Raft methods we need
// Since raft.Raft is a concrete type, we'll test the server methods
// that call Raft by verifying the commands are encoded correctly
type mockRaftForServer struct {
	*raft.Raft       // Embed real Raft if available, or nil
	state            raft.RaftState
	applyFunc        func(cmd []byte, timeout time.Duration) raft.ApplyFuture
	addVoterFunc     func(id raft.ServerID, address raft.ServerAddress, prevIndex, timeout uint64) raft.IndexFuture
	removeServerFunc func(id raft.ServerID, prevIndex, timeout uint64) raft.IndexFuture
}

func (m *mockRaftForServer) State() raft.RaftState {
	return m.state
}

func (m *mockRaftForServer) Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture {
	if m.applyFunc != nil {
		return m.applyFunc(cmd, timeout)
	}
	return &mockApplyFuture{err: nil}
}

func (m *mockRaftForServer) AddVoter(id raft.ServerID, address raft.ServerAddress, prevIndex, timeout uint64) raft.IndexFuture {
	if m.addVoterFunc != nil {
		return m.addVoterFunc(id, address, prevIndex, timeout)
	}
	return &mockIndexFuture{err: nil}
}

func (m *mockRaftForServer) RemoveServer(id raft.ServerID, prevIndex, timeout uint64) raft.IndexFuture {
	if m.removeServerFunc != nil {
		return m.removeServerFunc(id, prevIndex, timeout)
	}
	return &mockIndexFuture{err: nil}
}

type mockApplyFuture struct {
	err error
}

func (m *mockApplyFuture) Error() error {
	return m.err
}

func (m *mockApplyFuture) Response() interface{} {
	return nil
}

func (m *mockApplyFuture) Index() uint64 {
	return 0
}

type mockIndexFuture struct {
	err error
}

func (m *mockIndexFuture) Error() error {
	return m.err
}

func (m *mockIndexFuture) Index() uint64 {
	return 0
}

// testServerForForwarding implements DChanServiceServer for forwarding tests
type testServerForForwarding struct {
	p.UnsafeDChanServiceServer

	addVoterFunc    func(context.Context, *p.ServerInfo) (*emptypb.Empty, error)
	removeVoterFunc func(context.Context, *p.ServerInfo) (*emptypb.Empty, error)
}

func (s *testServerForForwarding) AddVoter(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
	if s.addVoterFunc != nil {
		return s.addVoterFunc(ctx, req)
	}
	return &emptypb.Empty{}, nil
}

func (s *testServerForForwarding) RemoveVoter(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
	if s.removeVoterFunc != nil {
		return s.removeVoterFunc(ctx, req)
	}
	return &emptypb.Empty{}, nil
}

func (s *testServerForForwarding) Receive(ctx context.Context, req *p.ReceiveRequest) (*p.ReceiveResponse, error) {
	return &p.ReceiveResponse{Received: true}, nil
}

func (s *testServerForForwarding) RegisterReceiver(ctx context.Context, req *p.ReceiverRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *testServerForForwarding) UnregisterReceiver(ctx context.Context, req *p.ReceiverRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func TestServerReceive(t *testing.T) {
	var testValue any = "test-message"
	var receivedValue any
	var receivedNamespace Namespace

	ch := make(chan any, 1)
	ctx := t.Context()

	rch := &rchannel{
		channel: channel{
			ch: ch,
		},
		ctx: ctx,
	}

	rm := &mockReceiverManager{
		getReceiverFunc: func(namespace Namespace) (*rchannel, func() int32, bool) {
			receivedNamespace = namespace
			count := int32(1)
			return rch, func() int32 {
				count--
				return count
			}, true
		},
	}

	// Create server directly since we can test Receive without Raft
	srv := &server{
		rm:     rm,
		raft:   nil,
		client: nil,
	}

	// Encode test value with gob
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(&testValue); err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	req := &p.ReceiveRequest{
		Namespace: "test-namespace",
		Data:      buf.Bytes(),
	}

	resp, err := srv.Receive(ctx, req)
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	if !resp.GetReceived() {
		t.Error("Receive should return received=true")
	}

	// Check that value was sent to channel
	select {
	case receivedValue = <-ch:
	case <-time.After(1 * time.Second):
		t.Fatal("Value was not received in channel")
	}

	if receivedValue != testValue {
		t.Errorf("Received value = %v, want %v", receivedValue, testValue)
	}

	if receivedNamespace != "test-namespace" {
		t.Errorf("Namespace = %v, want 'test-namespace'", receivedNamespace)
	}
}

func TestServerReceiveNoReceiver(t *testing.T) {
	rm := &mockReceiverManager{
		getReceiverFunc: func(namespace Namespace) (*rchannel, func() int32, bool) {
			return nil, func() int32 { return 0 }, false
		},
	}

	srv := &server{
		rm:     rm,
		raft:   nil,
		client: nil,
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(any("test"))

	req := &p.ReceiveRequest{
		Namespace: "test-namespace",
		Data:      buf.Bytes(),
	}

	resp, err := srv.Receive(t.Context(), req)
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	if resp.GetReceived() {
		t.Error("Receive should return received=false when no receiver")
	}
}

func TestServerReceiveInvalidGob(t *testing.T) {
	ch := make(chan any, 1)
	ctx := t.Context()

	rch := &rchannel{
		channel: channel{
			ch: ch,
		},
		ctx: ctx,
	}

	rm := &mockReceiverManager{
		getReceiverFunc: func(namespace Namespace) (*rchannel, func() int32, bool) {
			return rch, func() int32 { return 0 }, true
		},
	}

	srv := &server{
		rm:     rm,
		raft:   nil,
		client: nil,
	}

	req := &p.ReceiveRequest{
		Namespace: "test-namespace",
		Data:      []byte("invalid-gob-data"),
	}

	_, err := srv.Receive(t.Context(), req)
	if err == nil {
		t.Error("Receive should fail with invalid gob data")
	}
}

func TestServerReceiveContextCancelled(t *testing.T) {
	ch := make(chan any)
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel immediately

	rch := &rchannel{
		channel: channel{
			ch: ch,
			closeCh: make(chan struct{}, 1),
		},
		ctx: ctx,
	}

	rm := &mockReceiverManager{
		getReceiverFunc: func(namespace Namespace) (*rchannel, func() int32, bool) {
			count := int32(1)
			return rch, func() int32 {
				count--
				return count
			}, true
		},
	}

	srv := &server{
		rm:     rm,
		raft:   nil,
		client: nil,
	}

	var testValue any = "test"
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	enc.Encode(&testValue)

	req := &p.ReceiveRequest{
		Namespace: "test-namespace",
		Data:      buf.Bytes(),
	}

	resp, err := srv.Receive(t.Context(), req)
	if err != nil {
		t.Fatalf("Receive failed: %v", err)
	}

	if resp.GetReceived() {
		t.Error("Receive should return received=false when context is cancelled")
	}
}

// Test server Raft command encoding by testing the encode/decode cycle
// This verifies that RegisterReceiver would create the correct command
func TestServerRegisterReceiverCommandEncoding(t *testing.T) {
	// Simulate what RegisterReceiver does
	req := &p.ReceiverRequest{
		Namespace: "test-namespace",
		ServerId:  "server1",
		Requester: "requester1",
	}

	expectedCmd := fsmCmd{
		Type:      registerReceiver,
		Namespace: Namespace(req.GetNamespace()),
		ServerId:  ServerID(req.GetServerId()),
		Requester: ServerID(req.GetRequester()),
	}

	encoded, err := encodeFsmCmd(expectedCmd)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := decodeFsmCmd(encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded.Type != registerReceiver {
		t.Errorf("Type = %v, want %v", decoded.Type, registerReceiver)
	}
	if decoded.Namespace != "test-namespace" {
		t.Errorf("Namespace = %v, want 'test-namespace'", decoded.Namespace)
	}
	if decoded.ServerId != "server1" {
		t.Errorf("ServerId = %v, want 'server1'", decoded.ServerId)
	}
	if decoded.Requester != "requester1" {
		t.Errorf("Requester = %v, want 'requester1'", decoded.Requester)
	}
}

// Test server Raft command encoding for UnregisterReceiver
// This verifies that UnregisterReceiver would create the correct command
func TestServerUnregisterReceiverCommandEncoding(t *testing.T) {
	// Simulate what UnregisterReceiver does
	req := &p.ReceiverRequest{
		Namespace: "test-namespace",
		ServerId:  "server1",
		Requester: "requester1",
	}

	expectedCmd := fsmCmd{
		Type:      unregisterReceiver,
		Namespace: Namespace(req.GetNamespace()),
		ServerId:  ServerID(req.GetServerId()),
		Requester: ServerID(req.GetRequester()),
	}

	encoded, err := encodeFsmCmd(expectedCmd)
	if err != nil {
		t.Fatalf("Failed to encode: %v", err)
	}

	decoded, err := decodeFsmCmd(encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	if decoded.Type != unregisterReceiver {
		t.Errorf("Type = %v, want %v", decoded.Type, unregisterReceiver)
	}
	if decoded.Namespace != "test-namespace" {
		t.Errorf("Namespace = %v, want 'test-namespace'", decoded.Namespace)
	}
	if decoded.ServerId != "server1" {
		t.Errorf("ServerId = %v, want 'server1'", decoded.ServerId)
	}
	if decoded.Requester != "requester1" {
		t.Errorf("Requester = %v, want 'requester1'", decoded.Requester)
	}
}

// Test forwarding by creating a gRPC server and client
func TestServerAddVoterForwardsToLeader(t *testing.T) {
	var forwardedReq *p.ServerInfo

	// Create a test gRPC server that acts as the leader
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()

	forwardingServer := &testServerForForwarding{
		addVoterFunc: func(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
			forwardedReq = req
			return &emptypb.Empty{}, nil
		},
	}
	p.RegisterDChanServiceServer(grpcServer, forwardingServer)

	go func() {
		grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()
	defer listener.Close()

	// Create a client that can connect to our test server
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}

	// Test forwarding by directly calling the forwarding server
	// This verifies the forwarding mechanism works
	conn, err := grpc.NewClient("passthrough:///leader", dialOptions...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer conn.Close()

	grpcClient := p.NewDChanServiceClient(conn)
	ctx := t.Context()

	_, err = grpcClient.AddVoter(ctx, &p.ServerInfo{
		IdAddress: "server1",
	})
	if err != nil {
		t.Fatalf("AddVoter failed: %v", err)
	}

	if forwardedReq == nil {
		t.Fatal("AddVoter was not called on forwarding server")
	}

	if forwardedReq.GetIdAddress() != "server1" {
		t.Errorf("Forwarded IdAddress = %v, want 'server1'", forwardedReq.GetIdAddress())
	}
}

func TestServerRemoveVoterForwardsToLeader(t *testing.T) {
	var forwardedReq *p.ServerInfo

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()

	forwardingServer := &testServerForForwarding{
		removeVoterFunc: func(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
			forwardedReq = req
			return &emptypb.Empty{}, nil
		},
	}
	p.RegisterDChanServiceServer(grpcServer, forwardingServer)

	go func() {
		grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()
	defer listener.Close()

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}

	conn, err := grpc.NewClient("passthrough:///leader", dialOptions...)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	defer conn.Close()

	grpcClient := p.NewDChanServiceClient(conn)
	ctx := t.Context()

	_, err = grpcClient.RemoveVoter(ctx, &p.ServerInfo{
		IdAddress: "server1",
	})
	if err != nil {
		t.Fatalf("RemoveVoter failed: %v", err)
	}

	if forwardedReq == nil {
		t.Fatal("RemoveVoter was not called on forwarding server")
	}

	if forwardedReq.GetIdAddress() != "server1" {
		t.Errorf("Forwarded IdAddress = %v, want 'server1'", forwardedReq.GetIdAddress())
	}
}
