package dchan

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
	"github.com/m4tth3/dchan/proto"
	transport "github.com/m4tth3/dchan/transport"
	"google.golang.org/grpc"
)

type RpcClient = proto.DChanServiceClient

type ServerId string
type Namespace string

type Chan[T any] interface {
	Send(namespace Namespace, ctx context.Context) (chan<- T, context.CancelFunc, error)
	Broadcast(namespace Namespace, ctx context.Context) (chan<- T, context.CancelFunc, error)
	Receive(namespace Namespace, ctx context.Context) (<-chan T, context.CancelFunc, error)
}

type chanStream[T any] struct {
	ch chan T
	refCount atomic.Uint32
}

// dChan is a distributed channel that can be used to send and receive messages between nodes.
// It is a wrapper around a map of dChanStreams, one for each namespace.
type dChan[T any] struct {
	mu sync.Mutex

	// Connections to all the servers that are receiving.
	connections map[ServerId]RpcClient

	// Servers that are receiving from this channel.
	receivers map[Namespace][]ServerId

	// Streams that are open for sending or receiving. Streams are closed when
	// the last reference to the stream is closed.
	streams map[Namespace]*chanStream[T]

	raft *raft.Raft
}

func newRaft() {
	// https://github.com/Jille/raft-grpc-example/blob/master/main.go
	// https://github.com/hashicorp/raft-boltdb
	// https://github.com/hashicorp/raft
	_ = transport.New(raft.ServerAddress("localhost:1234"), []grpc.DialOption{})
}
