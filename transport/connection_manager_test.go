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

	pool := NewConnectionManager(dialOptions...)
	defer pool.DisconnectAll()

	target := raft.ServerAddress("test:1234")
	if _, err := pool.Register(target); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	defer pool.Disconnect(target)

	conn1, ok := pool.GetClient(target)
	if !ok {
		t.Fatalf("GetClient failed")
	}

	conn2, ok := pool.GetClient(target)
	if !ok {
		t.Fatalf("GetClient failed")
	}

	// Should return the same connection
	if conn1 != conn2 {
		t.Error("GetClient should return the same connection for the same target")
	}

	_, ok = pool.GetClient("ERROR")
	if ok {
		t.Fatalf("GetClient should not return a connection for an error target")
	}
}

func TestDisconnect_RemovesConnection(t *testing.T) {
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

	pool := NewConnectionManager(dialOptions...)
	target := raft.ServerAddress("disconnect-test:1234")
	conn, err := pool.Register(target)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	// Disconnect should remove the connection
	err = pool.Disconnect(target)
	if err != nil {
		t.Fatalf("Disconnect failed: %v", err)
	}

	if conn.GetState() != connectivity.Shutdown {
		t.Errorf("connection should be shutdown after disconnect")
	}
}

func TestRegister_ReturnsSameInstance(t *testing.T) {
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

	pool := NewConnectionManager(dialOptions...)
	target := raft.ServerAddress("register-same:9999")

	conn1, err := pool.Register(target)
	if err != nil {
		t.Fatalf("First Register failed: %v", err)
	}
	conn2, err := pool.Register(target)
	if err != nil {
		t.Fatalf("Second Register failed: %v", err)
	}

	if conn1 != conn2 {
		t.Errorf("Register should return the same *grpc.ClientConn instance for same target")
	}

	pool.Disconnect(target)
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

	pool := NewConnectionManager(dialOptions...)

	// Create multiple connections
	target1 := raft.ServerAddress("target1")
	target2 := raft.ServerAddress("target2")
	target3 := raft.ServerAddress("target3")

	conn1, err := pool.Register(target1)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	conn2, err := pool.Register(target2)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	conn3, err := pool.Register(target3)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	// Close all
	pool.DisconnectAll()

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
