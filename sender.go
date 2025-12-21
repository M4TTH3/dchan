package dchan

import (
	"bytes"
	"context"
	"encoding/gob"
	"math/rand"

	"github.com/hashicorp/raft"
	p "github.com/m4tth3/dchan/proto"
)

// WaitingSend is a struct that can be used to send a message with a wait
// until the message is sent. The context is the time to send a message.
//
// Warning: Using a client wait should be used with caution because it could
// block forever (e.g. servers crash). Set a timeout to avoid blocking forever.
// 
// The object must be sent through the channel
type WaitingSend struct {
	value any

	// context for the gRPC call e.g. how long it should live for
	sendCtx context.Context

	// lifetime of the send e.g. when it's finished error or not
	ctx  context.Context
	done context.CancelFunc
}

func (w WaitingSend) Done() <-chan struct{} {
	return w.ctx.Done()
}

type sender struct {
	dchan *dChan
	chann *channel

	i uint32
}

// send sends the message in a semi-round-robin fashion.
//
// This function provides backpressure to the sender and
// at most once semantics.
func (s *sender) send(v any, ctx context.Context) {
	var target ServerId

	for {
		s.i++ // Increment to "round-robin" as best as possible

		s.dchan.rcmu.RLock()
		externalChannel := s.dchan.getExternalChannel(s.chann.namespace)
		// We should block until we have at least one target.
		if len(externalChannel.servers) == 0 {
			// We have to wait and baton pass the write lock from the StateMachine to here
			externalChannel.waitCount.Add(1)
			s.dchan.rcmu.RUnlock()

			<-externalChannel.waitQueue // Wait (order doesn't matter)

			// After this point we have the WRITE Lock with an id reachable
			externalChannel.waitCount.Add(-1)
			target = externalChannel.servers[s.i%uint32(len(externalChannel.servers))]

			// Check if we can pass the baton again
			if externalChannel.waitCount.Load() > 0 {
				externalChannel.waitQueue <- struct{}{}
			} else {
				s.dchan.rcmu.Unlock()
			}
		} else {
			s.dchan.rcmu.RUnlock()
		}

		conn, ok := s.dchan.tm.ConnectionManager().Hijack(raft.ServerAddress(target)) // TODO: maybe switch to register
		if !ok {
			// Drop message if no connection is found.
			// TODO: Add Logging support
			return
		}

		client := p.NewDChanServiceClient(conn)

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(v); err != nil {
			// Drop message if encoding fails.
			// TODO: Add Logging support
			return
		}

		// User can add timeouts via grpc options.
		resp, err := client.Receive(ctx, &p.ReceiveRequest{})
		if err != nil {
			// Drop message if receiving fails.
			// TODO: Add Logging support
			return
		}

		// In the protocol, if the server rejects the message,
		// it should try the next server.
		if resp.GetReceived() {
			break
		}
	}
}

func (s *sender) start() {
	go func() {
		s.i = rand.Uint32() // Randomize the starting point (smart client behavior)
		for v := range s.chann.ch {
			obj, ok := v.(WaitingSend)
			if ok {
				s.send(obj.value, obj.sendCtx)
				obj.done()
			} else {
				s.send(v, context.Background())
			}
		}
	}()
}
