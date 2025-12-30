package dchan

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"

	"github.com/hashicorp/raft"
	p "github.com/m4tth3/dchan/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	ErrNoLongerReceiving = errors.New("no longer receiving")
)

type receiverManager interface {
	// Returns the receiver, a function to decrement the sending count, and a boolean
	// indicating if the receiver exists.
	//
	// If the receiver is grabbed, the sending count is incremented.
	// The function should be called when the message is sent to the receiver
	// or the context is done to decrement the sending count.
	getReceiver(namespace Namespace) (rch *rchannel, dec func() int32, ok bool)
}

type server struct {
	rm receiverManager
	raft *raft.Raft
	client *client

	p.UnsafeDChanServiceServer // Ensure compilation
}

var _ p.DChanServiceServer = &server{}

// TODO: make this support chunking instead
func (r server) Receive(ctx context.Context, req *p.ReceiveRequest) (*p.ReceiveResponse, error) {
	namespace := Namespace(req.GetNamespace())
	data := req.GetData()

	receiver, dec, ok := r.rm.getReceiver(namespace)
	if !ok {
		return &p.ReceiveResponse{Received: false}, nil
	}

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
		if count := dec(); count == 0 {
			// Last sender, notify close goroutine to close the channel.
			receiver.closeCh <- struct{}{}
		}

		return &p.ReceiveResponse{Received: false}, nil // Reject
	}

	dec()
	return &p.ReceiveResponse{Received: true}, nil
}

func (r server) RegisterReceiver(ctx context.Context, req *p.ReceiverRequest) (*emptypb.Empty, error) {
	cmd := fsmCmd{
		Type: registerReceiver,
		Namespace: Namespace(req.GetNamespace()),
		ServerId: ServerID(req.GetServerId()),
		Requester: ServerID(req.GetRequester()),
	}

	encodedCmd, err := encodeFsmCmd(cmd)
	if err != nil {
		return nil, err
	}

	future := r.raft.Apply(encodedCmd, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (r server) UnregisterReceiver(ctx context.Context, req *p.ReceiverRequest) (*emptypb.Empty, error) {
	cmd := fsmCmd{
		Type: unregisterReceiver,
		Namespace: Namespace(req.GetNamespace()),
		ServerId: ServerID(req.GetServerId()),
		Requester: ServerID(req.GetRequester()),
	}

	encodedCmd, err := encodeFsmCmd(cmd)
	if err != nil {
		return nil, err
	}

	future := r.raft.Apply(encodedCmd, 0)
	// Block until the command is applied.
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (r server) AddVoter(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
	if r.raft.State() != raft.Leader {
		if leader, err := r.client.getLeaderClient(); err != nil {
			return nil, err
		} else {
			// Forward the request to the leader.
			return leader.AddVoter(ctx, req)
		}
	}

	id := raft.ServerID(req.GetIdAddress())
	address := raft.ServerAddress(req.GetIdAddress())

	future := r.raft.AddVoter(id, address, 0, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (r server) RemoveVoter(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
	if r.raft.State() != raft.Leader {
		if leader, err := r.client.getLeaderClient(); err != nil {
			return nil, err
		} else {
			// Forward the request to the leader.
			return leader.RemoveVoter(ctx, req)
		}
	}

	id := raft.ServerID(req.GetIdAddress())

	future := r.raft.RemoveServer(id, 0, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}
