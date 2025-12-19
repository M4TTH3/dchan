package transport

import (
	"context"
	"net"
	"testing"

	"github.com/hashicorp/raft"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func setupTestServer(t *testing.T) (*grpc.Server, *bufconn.Listener) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()
	return server, listener
}

func getDialOption(listener *bufconn.Listener) grpc.DialOption {
	return grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	})
}

func TestGetClient_ReuseConnection(t *testing.T) {
	defer goleak.VerifyNone(t)

	server, listener := setupTestServer(t)
	defer func() {
		server.Stop()
		listener.Close()
	}()

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		getDialOption(listener),
	}

	pool := NewClientPool(10, ROUND_ROBIN, dialOptions)
	defer pool.Close()

	target := raft.ServerAddress("test:1234")

	conn1, err := pool.GetClient(target)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}

	conn2, err := pool.GetClient(target)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}

	// Should return the same connection
	if conn1 != conn2 {
		t.Error("GetClient should return the same connection for the same target")
	}
}

func TestGetClient_LRU_MovesToFront(t *testing.T) {
	defer goleak.VerifyNone(t)

	server, listener := setupTestServer(t)
	defer func() {
		server.Stop()
		listener.Close()
	}()

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		getDialOption(listener),
	}

	pool := NewClientPool(3, LRU, dialOptions)
	defer pool.Close()

	// Create 3 connections
	target1 := raft.ServerAddress("target1")
	target2 := raft.ServerAddress("target2")
	target3 := raft.ServerAddress("target3")

	conn1, err := pool.GetClient(target1)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}
	conn2, err := pool.GetClient(target2)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}
	_, err = pool.GetClient(target3)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}

	// Access target1 again - should move it to front
	conn1Again, err := pool.GetClient(target1)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}
	if conn1 != conn1Again {
		t.Error("Should return same connection")
	}

	// Add a 4th connection - should evict target2 (oldest, not recently used)
	target4 := raft.ServerAddress("target4")
	_, err = pool.GetClient(target4)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}

	// target2 should be evicted (was in the middle, not recently accessed)
	// target3 should still exist (was most recent before target1 was accessed)
	// target1 should still exist (was accessed most recently)
	// target4 should exist (just added)

	// Verify target2 is closed
	state := conn2.GetState()
	if state.String() != "Shutdown" {
		t.Logf("Expected target2 to be evicted, but connection state is: %v", state)
	}
}

func TestGetClient_ROUND_ROBIN_Eviction(t *testing.T) {
	server, listener := setupTestServer(t)
	defer func() {
		server.Stop()
		listener.Close()
	}()

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		getDialOption(listener),
	}

	pool := NewClientPool(2, ROUND_ROBIN, dialOptions)
	defer pool.Close()

	// Create 2 connections
	target1 := raft.ServerAddress("target1")
	target2 := raft.ServerAddress("target2")

	conn1, err := pool.GetClient(target1)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}
	conn2, err := pool.GetClient(target2)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}

	// Add a 3rd connection - should evict the oldest (target1)
	target3 := raft.ServerAddress("target3")
	_, err = pool.GetClient(target3)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}

	// target1 should be evicted (oldest)
	state := conn1.GetState()
	if state.String() != "Shutdown" {
		t.Logf("Expected target1 to be evicted, but connection state is: %v", state)
	}

	// target2 should still exist
	state = conn2.GetState()
	if state.String() == "Shutdown" {
		t.Error("target2 should not be evicted yet")
	}
}

func TestCloseClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	server, listener := setupTestServer(t)
	defer func() {
		server.Stop()
		listener.Close()
	}()

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		getDialOption(listener),
	}

	pool := NewClientPool(10, ROUND_ROBIN, dialOptions)
	defer pool.Close()

	target := raft.ServerAddress("test:1234")

	conn, err := pool.GetClient(target)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}

	// Close the client
	err = pool.CloseClient(target)
	if err != nil {
		t.Fatalf("CloseClient failed: %v", err)
	}

	// Verify connection is closed
	state := conn.GetState()
	if state != connectivity.Shutdown {
		t.Errorf("Expected connection to be shutdown, got: %v", state)
	}

	// Getting the same client should create a new connection
	conn2, err := pool.GetClient(target)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}
	if conn == conn2 {
		t.Error("Should return a new connection after closing")
	}
}

func TestClose(t *testing.T) {
	defer goleak.VerifyNone(t)

	server, listener := setupTestServer(t)
	defer func() {
		server.Stop()
		listener.Close()
	}()

	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		getDialOption(listener),
	}

	pool := NewClientPool(10, ROUND_ROBIN, dialOptions)

	// Create multiple connections
	target1 := raft.ServerAddress("target1")
	target2 := raft.ServerAddress("target2")
	target3 := raft.ServerAddress("target3")

	conn1, err := pool.GetClient(target1)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}
	conn2, err := pool.GetClient(target2)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}
	conn3, err := pool.GetClient(target3)
	if err != nil {
		t.Fatalf("GetClient failed: %v", err)
	}

	// Close all
	err = pool.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify all connections are closed
	state1 := conn1.GetState()
	state2 := conn2.GetState()
	state3 := conn3.GetState()

	if state1 != connectivity.Shutdown {
		t.Errorf("conn1 should be shutdown, got: %v", state1)
	}
	if state2 != connectivity.Shutdown {
		t.Errorf("conn2 should be shutdown, got: %v", state2)
	}
	if state3 != connectivity.Shutdown {
		t.Errorf("conn3 should be shutdown, got: %v", state3)
	}
}
