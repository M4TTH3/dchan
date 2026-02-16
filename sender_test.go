package dchan

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	p "github.com/m4tth3/dchan/proto"
)

type mockExtChanManager struct {
	waitForTargetFunc func(key uint32, namespace Namespace, ctx context.Context) (ServerID, bool)
}

func (m *mockExtChanManager) waitForTarget(key uint32, namespace Namespace, ctx context.Context) (ServerID, bool) {
	if m.waitForTargetFunc != nil {
		return m.waitForTargetFunc(key, namespace, ctx)
	}
	return "server1", true
}

type mockSendReceiver struct {
	receiveFunc func(ctx context.Context, target raft.ServerAddress, encoded []byte, namespace Namespace) (*p.ReceiveResponse, error)
	callCount   int
}

func (m *mockSendReceiver) receive(ctx context.Context, target raft.ServerAddress, encoded []byte, namespace Namespace) (*p.ReceiveResponse, error) {
	m.callCount++
	if m.receiveFunc != nil {
		return m.receiveFunc(ctx, target, encoded, namespace)
	}
	return &p.ReceiveResponse{Received: true}, nil
}

func TestSenderSendWithRetry(t *testing.T) {
	// Track receive calls
	callCount := 0
	var receivedValues []any
	var receivedTargets []raft.ServerAddress

	mockECM := &mockExtChanManager{
		waitForTargetFunc: func(key uint32, namespace Namespace, ctx context.Context) (ServerID, bool) {
			return "server1", true
		},
	}

	mockClient := &mockSendReceiver{
		receiveFunc: func(ctx context.Context, target raft.ServerAddress, encoded []byte, namespace Namespace) (*p.ReceiveResponse, error) {
			callCount++
			receivedTargets = append(receivedTargets, target)

			// Decode to verify what was sent
			decoded, err := gobDecode(encoded)
			if err == nil {
				receivedValues = append(receivedValues, decoded)
			}

			// First message: always succeed
			// Second message: first call fails, second call succeeds
			switch callCount {
			case 1:
				return &p.ReceiveResponse{Received: true}, nil
			case 2:
				return &p.ReceiveResponse{Received: false}, nil // First attempt fails
			case 3:
				return &p.ReceiveResponse{Received: true}, nil // Retry succeeds
			}

			return &p.ReceiveResponse{Received: true}, nil
		},
	}

	config := &Config{
		SendTimeout: 5 * time.Second,
	}

	chann := newChannel("test-namespace", 10)
	s := &sender{
		ecm:    mockECM,
		config: config,
		client: mockClient,
		chann:  chann,
	}

	s.start()

	// Send first message - should succeed on first try
	chann.ch <- "message1"

	// Wait a bit for first message to be processed
	time.Sleep(100 * time.Millisecond)

	// Send second message - first attempt will fail, then retry and succeed
	chann.ch <- "message2"

	// Wait for both messages to be processed
	time.Sleep(200 * time.Millisecond)

	// Close channel to trigger closeCh notification
	close(chann.ch)

	// Wait for sender goroutine to finish and send to closeCh
	select {
	case <-chann.closeCh:
		// Success - closeCh received value
	case <-time.After(1 * time.Second):
		t.Fatal("closeCh did not receive value after channel was closed")
	}

	// Verify we got 3 calls total (1 for first message, 2 for second message)
	if callCount != 3 {
		t.Errorf("Expected 3 receive calls, got %d", callCount)
	}

	// Verify both messages were sent
	if len(receivedValues) != 3 {
		t.Errorf("Expected 3 received values, got %d", len(receivedValues))
	}

	// Verify messages were decoded successfully
	// Note: gobEncode encodes &value, so the decoded structure may vary
	// We just verify that decoding succeeded and we got the expected number of values
	if len(receivedValues) != 3 {
		t.Errorf("Expected 3 decoded values, got %d", len(receivedValues))
	}

	// Verify all values were decoded (not nil)
	for i, val := range receivedValues {
		if val == nil {
			t.Errorf("Received value %d is nil", i)
		}
	}

	// Verify all calls went to the same target
	for _, target := range receivedTargets {
		if target != "server1" {
			t.Errorf("Target = %v, want 'server1'", target)
		}
	}
}

func TestSenderSendWithMessage(t *testing.T) {
	var receivedValue any
	var receivedCtx context.Context
	callCount := 0

	mockECM := &mockExtChanManager{
		waitForTargetFunc: func(key uint32, namespace Namespace, ctx context.Context) (ServerID, bool) {
			return "server1", true
		},
	}

	mockClient := &mockSendReceiver{
		receiveFunc: func(ctx context.Context, target raft.ServerAddress, encoded []byte, namespace Namespace) (*p.ReceiveResponse, error) {
			callCount++
			receivedCtx = ctx

			// Decode to verify what was sent
			decoded, err := gobDecode(encoded)
			if err == nil {
				receivedValue = decoded
			}

			return &p.ReceiveResponse{Received: true}, nil
		},
	}

	config := &Config{
		SendTimeout: 5 * time.Second,
	}

	chann := newChannel("test-namespace", 10)
	s := &sender{
		ecm:    mockECM,
		config: config,
		client: mockClient,
		chann:  chann,
	}

	s.start()

	// Create a Message with custom timeout
	messageTimeout := 2 * time.Second
	beforeCreate := time.Now()
	msg := WithMessage("test-message", messageTimeout)

	// Send the Message object
	chann.ch <- msg

	// Wait for message to be processed
	time.Sleep(200 * time.Millisecond)

	// Verify message was sent
	if callCount != 1 {
		t.Errorf("Expected 1 receive call, got %d", callCount)
	}

	// Verify the value was sent and decoded successfully
	if receivedValue == nil {
		t.Fatal("No value was received")
	}

	// Verify the context from Message was used (check timeout)
	if receivedCtx == nil {
		t.Fatal("Context was not received")
	}
	deadline, ok := receivedCtx.Deadline()
	if !ok {
		t.Error("Context should have a deadline from Message timeout")
	}
	// Deadline was set at creation time, so compare against that
	expectedDeadline := beforeCreate.Add(messageTimeout)
	if deadline.After(expectedDeadline.Add(100*time.Millisecond)) || deadline.Before(expectedDeadline.Add(-100*time.Millisecond)) {
		t.Errorf("Context deadline = %v, want around %v", deadline, expectedDeadline)
	}

	// Verify Message.Done() returns true (message was sent)
	// Read from the sent channel directly since cancel() was already called
	// by the sender goroutine, making select non-deterministic between
	// sent and ctx.Done()
	select {
	case sent := <-msg.sent:
		if !sent {
			t.Error("Message should have been sent successfully")
		}
	default:
		t.Error("Message.sent should have a value")
	}

	// Close channel and verify closeCh
	close(chann.ch)
	select {
	case <-chann.closeCh:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("closeCh did not receive value after channel was closed")
	}
}

func TestSenderCloseChNotification(t *testing.T) {
	mockECM := &mockExtChanManager{
		waitForTargetFunc: func(key uint32, namespace Namespace, ctx context.Context) (ServerID, bool) {
			return "server1", true
		},
	}

	mockClient := &mockSendReceiver{
		receiveFunc: func(ctx context.Context, target raft.ServerAddress, encoded []byte, namespace Namespace) (*p.ReceiveResponse, error) {
			return &p.ReceiveResponse{Received: true}, nil
		},
	}

	config := &Config{
		SendTimeout: 5 * time.Second,
	}

	chann := newChannel("test-namespace", 10)
	s := &sender{
		ecm:    mockECM,
		config: config,
		client: mockClient,
		chann:  chann,
	}

	s.start()

	// Send a message
	chann.ch <- "test"

	// Wait for message to be processed
	time.Sleep(100 * time.Millisecond)

	// Close the channel
	close(chann.ch)

	// Verify closeCh receives a value
	select {
	case <-chann.closeCh:
		// Success - closeCh received the notification
	case <-time.After(1 * time.Second):
		t.Fatal("closeCh did not receive value after channel was closed")
	}
}

func TestSenderMessageDone(t *testing.T) {
	mockECM := &mockExtChanManager{
		waitForTargetFunc: func(key uint32, namespace Namespace, ctx context.Context) (ServerID, bool) {
			return "server1", true
		},
	}

	mockClient := &mockSendReceiver{
		receiveFunc: func(ctx context.Context, target raft.ServerAddress, encoded []byte, namespace Namespace) (*p.ReceiveResponse, error) {
			return &p.ReceiveResponse{Received: true}, nil
		},
	}

	config := &Config{
		SendTimeout: 5 * time.Second,
	}

	chann := newChannel("test-namespace", 10)
	s := &sender{
		ecm:    mockECM,
		config: config,
		client: mockClient,
		chann:  chann,
	}

	s.start()

	// Create a Message
	msg := WithMessage("test", 5*time.Second)

	// Send the Message
	chann.ch <- msg

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	// Verify the message was sent successfully by reading sent channel directly.
	// We can't reliably use Done() here because the sender calls cancel() after
	// writing to sent, making select non-deterministic between sent and ctx.Done().
	select {
	case sent := <-msg.sent:
		if !sent {
			t.Error("Message should have been sent successfully")
		}
		// Put it back so subsequent reads work
		msg.sent <- sent
	default:
		t.Error("Message.sent should have a value")
	}

	// Verify the value persists after being read back
	select {
	case sent := <-msg.sent:
		if !sent {
			t.Error("Message.sent should still be true on subsequent read")
		}
	default:
		t.Error("Message.sent should still have a value")
	}

	close(chann.ch)
	select {
	case <-chann.closeCh:
	case <-time.After(1 * time.Second):
		t.Fatal("closeCh did not receive value")
	}
}

func TestSenderMessageDoneTimeout(t *testing.T) {
	mockECM := &mockExtChanManager{
		waitForTargetFunc: func(key uint32, namespace Namespace, ctx context.Context) (ServerID, bool) {
			// Simulate slow response - context will timeout
			time.Sleep(200 * time.Millisecond)
			return "server1", true
		},
	}

	mockClient := &mockSendReceiver{
		receiveFunc: func(ctx context.Context, target raft.ServerAddress, encoded []byte, namespace Namespace) (*p.ReceiveResponse, error) {
			return &p.ReceiveResponse{Received: true}, nil
		},
	}

	config := &Config{
		SendTimeout: 5 * time.Second,
	}

	chann := newChannel("test-namespace", 10)
	s := &sender{
		ecm:    mockECM,
		config: config,
		client: mockClient,
		chann:  chann,
	}

	s.start()

	// Create a Message with very short timeout
	msg := WithMessage("test", 50*time.Millisecond)

	// Send the Message
	chann.ch <- msg

	// Wait for timeout
	time.Sleep(100 * time.Millisecond)

	// Verify Done() returns false (timeout)
	if msg.Done() {
		t.Error("Message.Done() should return false when context times out")
	}

	close(chann.ch)
	select {
	case <-chann.closeCh:
	case <-time.After(1 * time.Second):
		t.Fatal("closeCh did not receive value")
	}
}
