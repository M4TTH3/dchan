package dchan

import (
	"bytes"
	"context"
	"encoding/gob"
	"math/rand"
	"time"

	"github.com/hashicorp/raft"
	p "github.com/m4tth3/dchan/proto"
)

// Message is a struct that can be used to send a message with a wait
// until the message is sent or the context is done.
type Message struct {
	value any

	// context for the gRPC call e.g. how long it should live for
	ctx  context.Context
	cancel context.CancelFunc

	sent chan bool
}

// WithMessage creates a new Message object that can be used to send a message to wait
// until the message is sent or the timeout is reached.
//
// This timeout overrides the default SendTimeout in the Config.
//
// If timeout is non-zero, it will use the provided timeout else default timeout
func WithMessage(obj any, timeout time.Duration) Message {
	if timeout <= 0 {
		timeout = DefaultSendTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return Message{value: obj, ctx: ctx, cancel: cancel, sent: make(chan bool, 1)}
}

// Done waits until the message is sent or the context is done.
//
// Returns true if the message is guaranteed sent (e.g. target received it).
// False if the message is possibly sent or not at all (e.g. context is done)
//
// Guarantees at most once semantics.
func (w Message) Done() bool {
	select {
	case sent := <-w.sent:
		w.sent <- sent // Pass it to next
		return sent
	case <-w.ctx.Done():
		return false
	}
}

type sender struct {
	dchan *Chan
	chann *channel

	i uint32
}

// send sends the message in a semi-round-robin fashion.
//
// This function provides backpressure to the sender and
// at most once semantics.
//
// TODO: support sending the encoded as chunks.
// TODO: add logging support
func (s *sender) send(v any, ctx context.Context) bool {
	var target ServerId

	for {
		s.i++ // Increment to "round-robin" as best as possible

		s.dchan.extmu.RLock()
		externalChannel := s.dchan.getExternalChannel(s.chann.namespace)

		// We should block until we have at least one target.
		if externalChannel.servers.len() == 0 {
			// We have to wait and baton pass the write lock from the StateMachine to here
			externalChannel.waitCount.Add(1)
			s.dchan.extmu.RUnlock()

			<-externalChannel.waitQueue // Wait (order doesn't matter)

			// After this point we have the WRITE Lock with an id reachable
			externalChannel.waitCount.Add(-1)
			serverLen := uint32(externalChannel.servers.len())
			target, _ = externalChannel.servers.get(int(s.i % serverLen))

			// Check if we can pass the baton again
			if externalChannel.waitCount.Load() > 0 {
				externalChannel.waitQueue <- struct{}{}
			} else {
				s.dchan.extmu.Unlock()
			}
		} else {
			s.dchan.extmu.RUnlock()
		}

		client, err := s.dchan.getClient(raft.ServerAddress(target))
		if err != nil {
			return false
		}

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(v); err != nil {
			return false
		}

		// User can add timeouts via grpc options.
		resp, err := client.Receive(ctx, &p.ReceiveRequest{})
		if err != nil {
			return false
		}

		// In the protocol, if the server rejects the message,
		// it should try the next server.
		if resp.GetReceived() {
			break
		}
	}

	return true
}

func (s *sender) start() {
	go func() {
		s.i = rand.Uint32() // Randomize the starting point (smart client behavior)
		for v := range s.chann.ch {
			obj, ok := v.(Message)
			if ok {
				obj.sent <- s.send(obj.value, obj.ctx)
				obj.cancel()
			} else {
				// TODO: maybe we set this right before the request
				ctx, cancel := s.dchan.sendCtx()
				s.send(v, ctx)
				cancel()
			}
		}

		// Notify it's finished.
		s.chann.closeCh <- struct{}{}
	}()
}
