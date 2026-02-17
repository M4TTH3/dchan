package dchan_test

import (
	"encoding/gob"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/m4tth3/dchan"
)

func init() {
	// Register types with gob so they can be encoded/decoded through any.
	gob.Register("")
	gob.Register(0)
}

// getFreeAddr returns a free TCP address on localhost.
func getFreeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to get free address: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func TestTwoServerSendReceive(t *testing.T) {
	addr1 := getFreeAddr(t)
	addr2 := getFreeAddr(t)

	dir1 := t.TempDir()
	dir2 := t.TempDir()
	clusterAddresses := []string{addr1, addr2}
	clusterId := "test-cluster"

	// Server 1 is the bootstrap node (first in clusterAddresses).
	server1, err := dchan.New(addr1, clusterId, clusterAddresses, dir1,
		dchan.WithSendTimeout(10*time.Second),
		dchan.WithClusterTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("Failed to create server1: %v", err)
	}
	// Note: defer server.Close().Wait() would call Close() immediately
	// because Go evaluates method chain arguments at defer time.
	defer func() { server1.Close().Wait() }()

	// Give server1 time to become leader.
	time.Sleep(3 * time.Second)

	// Server 2 joins the cluster.
	server2, err := dchan.New(addr2, clusterId, clusterAddresses, dir2,
		dchan.WithSendTimeout(10*time.Second),
		dchan.WithClusterTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("Failed to create server2: %v", err)
	}
	defer func() { server2.Close().Wait() }()

	// Give server2 time to join the cluster.
	time.Sleep(2 * time.Second)

	// Server 2 opens a receiver on "greetings" namespace.
	recvCh, closeRecv, err := server2.Receive("greetings", 10)
	if err != nil {
		t.Fatalf("server2.Receive failed: %v", err)
	}
	defer func() { closeRecv().Wait() }()

	// Give time for the receiver registration to propagate via Raft.
	time.Sleep(2 * time.Second)

	// Server 1 opens a sender on "greetings" namespace.
	sendCh, closeSend, err := server1.Send("greetings", 10)
	if err != nil {
		t.Fatalf("server1.Send failed: %v", err)
	}
	defer func() { closeSend().Wait() }()

	// Send a message from server1.
	sendCh <- "hello from server1"

	// Receive the message on server2.
	select {
	case msg := <-recvCh:
		str := fmt.Sprintf("%v", msg)
		t.Logf("Received message: %v (type %T)", msg, msg)
		if str != "hello from server1" {
			t.Errorf("Received = %v, want 'hello from server1'", str)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for message from server1")
	}
}

func TestTwoServerWithMessage(t *testing.T) {
	addr1 := getFreeAddr(t)
	addr2 := getFreeAddr(t)

	dir1 := t.TempDir()
	dir2 := t.TempDir()
	clusterAddresses := []string{addr1, addr2}
	clusterId := "test-cluster-msg"

	// Server 1 is the bootstrap node.
	server1, err := dchan.New(addr1, clusterId, clusterAddresses, dir1,
		dchan.WithSendTimeout(10*time.Second),
		dchan.WithClusterTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("Failed to create server1: %v", err)
	}
	defer func() { server1.Close().Wait() }()

	// Give server1 time to become leader.
	time.Sleep(3 * time.Second)

	// Server 2 joins the cluster.
	server2, err := dchan.New(addr2, clusterId, clusterAddresses, dir2,
		dchan.WithSendTimeout(10*time.Second),
		dchan.WithClusterTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("Failed to create server2: %v", err)
	}
	defer func() { server2.Close().Wait() }()

	// Give server2 time to join the cluster.
	time.Sleep(2 * time.Second)

	// Server 2 opens a receiver on "numbers" namespace.
	recvCh, closeRecv, err := server2.Receive("numbers", 10)
	if err != nil {
		t.Fatalf("server2.Receive failed: %v", err)
	}
	defer func() { closeRecv().Wait() }()

	// Give time for the receiver registration to propagate via Raft.
	time.Sleep(2 * time.Second)

	// Server 1 opens a sender on "numbers" namespace.
	sendCh, closeSend, err := server1.Send("numbers", 10)
	if err != nil {
		t.Fatalf("server1.Send failed: %v", err)
	}
	defer func() { closeSend().Wait() }()

	// Send using WithMessage for delivery confirmation.
	msg := dchan.WithMessage(42, 10*time.Second)
	sendCh <- msg

	// Verify message was received on server2.
	select {
	case received := <-recvCh:
		t.Logf("Received: %v (type %T)", received, received)
	case <-time.After(10 * time.Second):
		t.Fatal("Timed out waiting for message from server1")
	}

	// Check that the sender got delivery confirmation via Done().
	if !msg.Done() {
		t.Error("Message.Done() should return true after successful send")
	}
}
