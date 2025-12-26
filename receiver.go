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

type server struct {
	dchan *dChan

	p.UnsafeDChanServiceServer // Ensure compilation
}

var _ p.DChanServiceServer = &server{}

// TODO: make this support chunking instead
func (r server) Receive(ctx context.Context, req *p.ReceiveRequest) (*p.ReceiveResponse, error) {
	namespace := Namespace(req.GetNamespace())
	data := req.GetData()

	r.dchan.rmu.RLock()
	receiver, ok := r.dchan.receivers[namespace]; if !ok || receiver.closed {
		r.dchan.rmu.RUnlock()
		return &p.ReceiveResponse{Received: false}, nil
	}

	receiver.sendingCount.Add(1)
	r.dchan.rmu.RUnlock()

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

func (r server) RegisterReceiver(ctx context.Context, req *p.ReceiverRequest) (*emptypb.Empty, error) {
	cmd := fsmCmd{
		Type: registerReceiver,
		Namespace: Namespace(req.GetNamespace()),
		ServerId: ServerId(req.GetServerId()),
		Requester: ServerId(req.GetRequester()),
	}

	encodedCmd, err := encodeFsmCmd(cmd)
	if err != nil {
		return nil, err
	}

	future := r.dchan.raft.Apply(encodedCmd, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (r server) UnregisterReceiver(ctx context.Context, req *p.ReceiverRequest) (*emptypb.Empty, error) {
	cmd := fsmCmd{
		Type: unregisterReceiver,
		Namespace: Namespace(req.GetNamespace()),
		ServerId: ServerId(req.GetServerId()),
		Requester: ServerId(req.GetRequester()),
	}

	encodedCmd, err := encodeFsmCmd(cmd)
	if err != nil {
		return nil, err
	}

	future := r.dchan.raft.Apply(encodedCmd, 0)
	// Block until the command is applied.
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (r server) AddVoter(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
	if r.dchan.raft.State() != raft.Leader {
		if leader, err := r.dchan.getLeaderClient(); err != nil {
			return nil, err
		} else {
			// Forward the request to the leader.
			return leader.AddVoter(ctx, req)
		}
	}

	id := raft.ServerID(req.GetIdAddress())
	address := raft.ServerAddress(req.GetIdAddress())

	future := r.dchan.raft.AddVoter(id, address, 0, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (r server) RemoveVoter(ctx context.Context, req *p.ServerInfo) (*emptypb.Empty, error) {
	if r.dchan.raft.State() != raft.Leader {
		if leader, err := r.dchan.getLeaderClient(); err != nil {
			return nil, err
		} else {
			// Forward the request to the leader.
			return leader.RemoveVoter(ctx, req)
		}
	}

	id := raft.ServerID(req.GetIdAddress())

	future := r.dchan.raft.RemoveServer(id, 0, 0)
	if err := future.Error(); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}
