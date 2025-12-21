package dchan

import (
	"bytes"
	"context"
	"encoding/gob"
	"math/rand"

	"github.com/hashicorp/raft"
	p "github.com/m4tth3/dchan/proto"
)

type sender struct {
	dchan *dChan
	chann *channel

	i uint32
}

// send sends the message in a semi-round-robin fashion.
//
// This function provides backpressure to the sender and
// at most once semantics.
func (s *sender) send(v any) {
	var target ServerId

	for {
		s.i++ // Increment to "round-robin" as best as possible

		s.dchan.rcmu.Lock()
		externalChannel := s.dchan.getExternalChannel(s.chann.namespace)
		// We should block until we have at least one target.
		if len(externalChannel.servers) == 0 {
			// We have to wait and baton pass the write lock from the StateMachine to here
			baton := make(chan struct{})
			queueEntry := externalChannel.waitQueue.PushBack(baton)
			s.dchan.rcmu.Unlock()

			<-baton

			// After this point we have the write lock with an id reachable
			externalChannel.waitQueue.Remove(queueEntry)
			target = externalChannel.servers[s.i % uint32(len(externalChannel.servers))]

			// Scan through to see if we can pass the baton again
			if externalChannel.waitQueue.Len() > 0 {
				externalChannel.waitQueue.Front().Value.(chan struct{}) <- struct{}{}
			} else {
				s.dchan.rcmu.Unlock()
			}
		} else {
			s.dchan.rcmu.Unlock()
		}
		
		conn, ok := s.dchan.tm.ConnectionManager().Hijack(raft.ServerAddress(target))
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
		resp, err := client.Receive(context.Background(), &p.ReceiveRequest{})
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
			s.send(v)
		}
	}()
}