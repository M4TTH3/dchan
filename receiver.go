package dchan

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"

	p "github.com/m4tth3/dchan/proto"
)

var (
	ErrNoLongerReceiving = errors.New("no longer receiving")
)

type receiver struct {
	dchan *dChan

	p.UnsafeDChanServiceServer // Ensure compilation
}

var _ p.DChanServiceServer = &receiver{}

func (g receiver) Receive(ctx context.Context, req *p.ReceiveRequest) (*p.ReceiveResponse, error) {
	namespace := Namespace(req.GetNamespace())
	data := req.GetData()

	g.dchan.rmu.RLock()
	receiver, ok := g.dchan.receivers[namespace]; if !ok {
		g.dchan.rmu.RUnlock()
		return &p.ReceiveResponse{Received: false}, nil
	}

	receiver.sendingCount.Add(1)
	g.dchan.rmu.RUnlock()

	var v any
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&v); err != nil {
		return nil, err
	}

	// The message is sent to the channel when the receiver is ready to receive.
	// This allows for client backpressure.
	//
	// The client can explicitly set deadlines to avoid blocking.
	select {
	case receiver.ch <- v:
	case <-receiver.ctx.Done(): // No more receivers, stop sending.
		if count := receiver.sendingCount.Add(-1); count == 0 {
			// Last sender, notify close goroutine to close the channel.
			receiver.closeCh <- struct{}{}
		}

		return &p.ReceiveResponse{Received: false}, nil // Reject
	}

	receiver.sendingCount.Add(-1)

	return &p.ReceiveResponse{Received: true}, nil
}
