package dchan

import (
	"container/list"
	"context"
	"encoding/gob"
	"errors"
	"sync"

	"github.com/hashicorp/raft"
	p "github.com/m4tth3/dchan/proto"
	transport "github.com/m4tth3/dchan/transport"
	"google.golang.org/grpc"
)

type RpcClient = p.DChanServiceClient

type ServerId string
type Namespace string

type CloseFunc func()

type BufferSize int

type CustomEncodable interface {
	gob.Encoder
	gob.Decoder
}

// Chan is the interface for a distributed channel that can be used to send and receive messages between nodes.
type Chan interface {
	// Send returns a channel that can be used to send messages to the distributed channel.
	// Explicitly run CloseFunc when the channel is no longer needed.
	//
	// The bufferSize is Sender + Receiver buffer sizes.
	// Use a send deadline to avoid blocking if that's desired.
	//
	// If a local channel already exists, the same instance is returned (bufferSize is ignored).
	Send(namespace Namespace, bufferSize BufferSize) (chan<- any, CloseFunc, error)

	// TODO: Support in the future. Use an epidemic broadcast approach.
	// Broadcast(namespace Namespace, ctx context.Context) (chan<- T, context.CancelFunc, error)

	// Receive returns a channel that can be used to receive messages from the distributed channel.
	// Explicitly run CloseFunc to close the channel.
	//
	// If a local channel already exists, the same instance is returned (bufferSize is ignored).
	Receive(namespace Namespace, bufferSize BufferSize) (<-chan any, CloseFunc, error)

	Close() error
}

// channel is a local channel wrapper that tracks reference counts
type channel struct {
	namespace Namespace

	ch chan any
	refCount int

	ctx context.Context
	cancel context.CancelFunc
	closeFunc CloseFunc
}

// externalChannel contains information about a server that is receiving this channel.
type externalChannel struct {
	servers []ServerId

	// goroutines waiting to send
	waitQueue list.List
}

// dChan is a distributed channel that can be used to send and receive messages between nodes.
// It is a wrapper around a map of dChanStreams, one for each namespace.
type dChan struct {
	sync.RWMutex

	// Servers that are receiving this channel (open for receiving).
	// Used for senders to know which servers to send to.
	// Updated via Raft FSM.
	rcmu sync.Mutex
	externalChannels map[Namespace]*externalChannel

	// channels (local sender) that are open for sending from Send(...)
	smu sync.RWMutex
	senders map[Namespace]*channel

	// channels (local receiver) that are open for receiving from Receive(...)
	rmu sync.RWMutex
	receivers map[Namespace]*channel

	tm *transport.Manager
	raft *raft.Raft
}

type closeType int

const (
	closeSend closeType = iota
	closeReceive
)

func (d *dChan) Send(namespace Namespace, bufferSize BufferSize) (chan<- any, CloseFunc, error) {
	d.Lock()
	defer d.Unlock()

	if c, ok := d.senders[namespace]; ok {
		return c.ch, c.closeFunc, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	c := &channel{namespace: namespace, ch: make(chan any, bufferSize), refCount: 1, ctx: ctx, cancel: cancel}
	c.closeFunc = d.tryClose(c, closeSend)

	d.senders[namespace] = c

	// Create a goroutine to receive messages from the channel and send them through gRPC.

	return c.ch, c.closeFunc, nil
}

func (d *dChan) Receive(namespace Namespace, bufferSize BufferSize) (<-chan any, CloseFunc, error) {
	d.rmu.Lock()
	defer d.rmu.Unlock()

	if ch, ok := d.receivers[namespace]; ok {
		return ch.ch, d.tryClose(ch, closeReceive), nil
	}

	c := &channel{namespace: namespace, ch: make(chan any, bufferSize), refCount: 1}
	d.receivers[namespace] = c

	// Create a goroutine to receive messages from a gRPC stream.
	// Send Raft message to add Receiver to the channel.

	return c.ch, d.tryClose(c, closeReceive), nil
}

func (d *dChan) Close() error {
	d.Lock()
	defer d.Unlock()

	// TODO: Close all channels and remove from senders and receivers.
	future := d.raft.Shutdown()
	future.Error()

	return errors.New("not implemented")
}

// tryClose closes the channel if the reference count is 0.
func (d *dChan) tryClose(c *channel, closeType closeType) CloseFunc {
	return func() {
		d.Lock()
		defer d.Unlock()

		c.refCount--

		if c.refCount == 0 {
			switch closeType {
			case closeSend:
				delete(d.senders, c.namespace)
			case closeReceive:
				delete(d.receivers, c.namespace)
			}
		}
	}
}

// getExternalChannel gets the external channel for the given namespace.
// If it doesn't exist, it creates a new one.
//
// Caller must hold the rcmu lock.
func (d *dChan) getExternalChannel(namespace Namespace) *externalChannel {
	if _, ok := d.externalChannels[namespace]; !ok {
		d.externalChannels[namespace] = &externalChannel{servers: make([]ServerId, 0)}
	}
	
	return d.externalChannels[namespace]
}

func newRaft() {
	// https://github.com/Jille/raft-grpc-example/blob/master/main.go
	// https://github.com/hashicorp/raft-boltdb
	// https://github.com/hashicorp/raft
	_ = transport.New(raft.ServerAddress("localhost:1234"), []grpc.DialOption{})

}
