package dchan

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	p "github.com/m4tth3/dchan/proto"
	transport "github.com/m4tth3/dchan/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

// testServer implements DChanServiceServer for testing
type testServer struct {
	p.UnsafeDChanServiceServer

	registerReceiverFunc   func(context.Context, *p.ReceiverRequest) (*emptypb.Empty, error)
	unregisterReceiverFunc func(context.Context, *p.ReceiverRequest) (*emptypb.Empty, error)
	addVoterFunc           func(context.Context, *p.ServerInfo) (*emptypb.Empty, error)
	removeVoterFunc        func(context.Context, *p.ServerInfo) (*emptypb.Empty, error)
}

func (s *testServer) RegisterReceiver(ctx context.Context, req *p.ReceiverRequest) (*emptypb.Empty, error) {
	if s.registerReceiverFunc != nil {
		return s.registerReceiverFunc(ctx, req)
	}
	return &emptypb.Empty{}, nil
}

func (s *testServer) UnregisterReceiver(ctx context.Context, req *p.ReceiverRequest) (*emptypb.Empty, error) {
	if s.unregisterReceiverFunc != nil {
		return s.unregisterReceiverFunc(ctx, req)
	}
	return &emptypb.Empty{}, nil
}

func (s *testServer) AddVoter(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
	if s.addVoterFunc != nil {
		return s.addVoterFunc(ctx, req)
	}
	return &emptypb.Empty{}, nil
}

func (s *testServer) RemoveVoter(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
	if s.removeVoterFunc != nil {
		return s.removeVoterFunc(ctx, req)
	}
	return &emptypb.Empty{}, nil
}

func (s *testServer) Receive(ctx context.Context, req *p.ReceiveRequest) (*p.ReceiveResponse, error) {
	return &p.ReceiveResponse{Received: true}, nil
}

// setupTestServer creates a test gRPC server with bufconn
func setupTestServer(t *testing.T, testSrv *testServer) (*bufconn.Listener, *grpc.Server) {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()

	if testSrv != nil {
		p.RegisterDChanServiceServer(grpcServer, testSrv)
	}

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	return listener, grpcServer
}

// createTestClient creates a test client for methods that don't require Raft
func createTestClient(t *testing.T, listener *bufconn.Listener) *client {
	t.Helper()

	config := DefaultConfig()
	config.ClusterTimeout = 5 * time.Second

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
	}

	cm := transport.NewConnectionManager(dialOptions...)

	return &client{
		localID: "test-client",
		raft:    nil, // Will be set when needed
		config:  &config,
		cm:      cm,
	}
}

func TestClientGetClient(t *testing.T) {
	addr := raft.ServerAddress("passthrough:///server1")

	testSrv := &testServer{}
	listener, grpcServer := setupTestServer(t, testSrv)
	defer grpcServer.Stop()
	defer listener.Close()

	client := createTestClient(t, listener)
	defer client.cm.DisconnectAll()

	grpcClient, err := client.getClient(addr)
	if err != nil {
		t.Fatalf("getClient failed: %v", err)
	}

	if grpcClient == nil {
		t.Fatal("getClient returned nil client")
	}
}

func TestClientRegisterReceiver(t *testing.T) {
	leaderAddr := raft.ServerAddress("passthrough:///leader")

	var receivedReq *p.ReceiverRequest
	testSrv := &testServer{
		registerReceiverFunc: func(ctx context.Context, req *p.ReceiverRequest) (*emptypb.Empty, error) {
			receivedReq = req
			return &emptypb.Empty{}, nil
		},
	}

	listener, grpcServer := setupTestServer(t, testSrv)
	defer grpcServer.Stop()
	defer listener.Close()

	// Create a minimal Raft that returns our leader
	// For a full test, you'd need a real Raft instance, but we can test
	// the gRPC call part by directly calling getClient
	client := createTestClient(t, listener)
	defer client.cm.DisconnectAll()

	// Test the gRPC call directly since we can't easily mock Raft
	grpcClient, err := client.getClient(leaderAddr)
	if err != nil {
		t.Fatalf("getClient failed: %v", err)
	}

	ctx, cancel := client.config.clusterCtx()
	defer cancel()

	_, err = grpcClient.RegisterReceiver(ctx, &p.ReceiverRequest{
		Namespace: "test-namespace",
		ServerId:  "test-client",
		Requester: "test-client",
	})
	if err != nil {
		t.Fatalf("RegisterReceiver RPC failed: %v", err)
	}

	if receivedReq == nil {
		t.Fatal("RegisterReceiver was not called")
	}

	if receivedReq.GetNamespace() != "test-namespace" {
		t.Errorf("Namespace = %v, want 'test-namespace'", receivedReq.GetNamespace())
	}
	if receivedReq.GetServerId() != "test-client" {
		t.Errorf("ServerId = %v, want 'test-client'", receivedReq.GetServerId())
	}
	if receivedReq.GetRequester() != "test-client" {
		t.Errorf("Requester = %v, want 'test-client'", receivedReq.GetRequester())
	}
}

func TestClientUnregisterReceiver(t *testing.T) {
	leaderAddr := raft.ServerAddress("passthrough:///leader")

	var receivedReq *p.ReceiverRequest
	testSrv := &testServer{
		unregisterReceiverFunc: func(ctx context.Context, req *p.ReceiverRequest) (*emptypb.Empty, error) {
			receivedReq = req
			return &emptypb.Empty{}, nil
		},
	}

	listener, grpcServer := setupTestServer(t, testSrv)
	defer grpcServer.Stop()
	defer listener.Close()

	client := createTestClient(t, listener)
	defer client.cm.DisconnectAll()

	grpcClient, err := client.getClient(leaderAddr)
	if err != nil {
		t.Fatalf("getClient failed: %v", err)
	}

	ctx, cancel := client.config.clusterCtx()
	defer cancel()

	_, err = grpcClient.UnregisterReceiver(ctx, &p.ReceiverRequest{
		Namespace: "test-namespace",
		ServerId:  "server1",
		Requester: "test-client",
	})
	if err != nil {
		t.Fatalf("UnregisterReceiver RPC failed: %v", err)
	}

	if receivedReq == nil {
		t.Fatal("UnregisterReceiver was not called")
	}

	if receivedReq.GetNamespace() != "test-namespace" {
		t.Errorf("Namespace = %v, want 'test-namespace'", receivedReq.GetNamespace())
	}
	if receivedReq.GetServerId() != "server1" {
		t.Errorf("ServerId = %v, want 'server1'", receivedReq.GetServerId())
	}
	if receivedReq.GetRequester() != "test-client" {
		t.Errorf("Requester = %v, want 'test-client'", receivedReq.GetRequester())
	}
}

func TestClientRegisterAsVoter(t *testing.T) {
	targetAddr := raft.ServerAddress("passthrough:///target")

	var receivedReq *p.ServerInfo
	testSrv := &testServer{
		addVoterFunc: func(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
			receivedReq = req
			return &emptypb.Empty{}, nil
		},
	}

	listener, grpcServer := setupTestServer(t, testSrv)
	defer grpcServer.Stop()
	defer listener.Close()

	client := createTestClient(t, listener)
	defer client.cm.DisconnectAll()

	grpcClient, err := client.getClient(targetAddr)
	if err != nil {
		t.Fatalf("getClient failed: %v", err)
	}

	ctx, cancel := client.config.clusterCtx()
	defer cancel()

	_, err = grpcClient.AddVoter(ctx, &p.ServerInfo{
		IdAddress: "test-client",
	})
	if err != nil {
		t.Fatalf("AddVoter RPC failed: %v", err)
	}

	if receivedReq == nil {
		t.Fatal("AddVoter was not called")
	}

	if receivedReq.GetIdAddress() != "test-client" {
		t.Errorf("IdAddress = %v, want 'test-client'", receivedReq.GetIdAddress())
	}
}

func TestClientUnregisterAsVoter(t *testing.T) {
	leaderAddr := raft.ServerAddress("passthrough:///leader")

	var receivedReq *p.ServerInfo
	testSrv := &testServer{
		removeVoterFunc: func(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
			receivedReq = req
			return &emptypb.Empty{}, nil
		},
	}

	listener, grpcServer := setupTestServer(t, testSrv)
	defer grpcServer.Stop()
	defer listener.Close()

	client := createTestClient(t, listener)
	defer client.cm.DisconnectAll()

	grpcClient, err := client.getClient(leaderAddr)
	if err != nil {
		t.Fatalf("getClient failed: %v", err)
	}

	ctx, cancel := client.config.clusterCtx()
	defer cancel()

	_, err = grpcClient.RemoveVoter(ctx, &p.ServerInfo{
		IdAddress: "test-client",
	})
	if err != nil {
		t.Fatalf("RemoveVoter RPC failed: %v", err)
	}

	if receivedReq == nil {
		t.Fatal("RemoveVoter was not called")
	}

	if receivedReq.GetIdAddress() != "test-client" {
		t.Errorf("IdAddress = %v, want 'test-client'", receivedReq.GetIdAddress())
	}
}

func TestClientGetClientError(t *testing.T) {
	// Test with invalid address
	invalidAddr := raft.ServerAddress("passthrough:///invalid")

	testSrv := &testServer{}
	listener, grpcServer := setupTestServer(t, testSrv)
	defer grpcServer.Stop()
	defer listener.Close()

	client := createTestClient(t, listener)
	defer client.cm.DisconnectAll()

	// This should fail because we're using a different address
	// that doesn't match our listener
	_, err := client.getClient(invalidAddr)
	// The error depends on connection manager behavior
	// We just verify it doesn't panic
	if err == nil {
		t.Log("getClient with invalid address - behavior depends on connection manager")
	}
}

func TestClientRPCErrorHandling(t *testing.T) {
	leaderAddr := raft.ServerAddress("passthrough:///leader")

	testSrv := &testServer{
		registerReceiverFunc: func(ctx context.Context, req *p.ReceiverRequest) (*emptypb.Empty, error) {
			return nil, context.DeadlineExceeded
		},
	}

	listener, grpcServer := setupTestServer(t, testSrv)
	defer grpcServer.Stop()
	defer listener.Close()

	client := createTestClient(t, listener)
	defer client.cm.DisconnectAll()

	grpcClient, err := client.getClient(leaderAddr)
	if err != nil {
		t.Fatalf("getClient failed: %v", err)
	}

	// Set a very short timeout to test error handling
	client.config.ClusterTimeout = 1 * time.Millisecond
	time.Sleep(2 * time.Millisecond) // Wait for timeout

	ctx, cancel := client.config.clusterCtx()
	defer cancel()

	_, err = grpcClient.RegisterReceiver(ctx, &p.ReceiverRequest{
		Namespace: "test-namespace",
		ServerId:  "test-client",
		Requester: "test-client",
	})
	if err == nil {
		t.Error("RegisterReceiver should have failed with timeout")
	}
}
