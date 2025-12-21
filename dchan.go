package dchan

import (
	"context"
	"encoding/gob"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/raft"
	p "github.com/m4tth3/dchan/proto"
	transport "github.com/m4tth3/dchan/transport"
	"google.golang.org/grpc"
)

type RpcClient = p.DChanServiceClient

type ServerId string
type Namespace string

type Future = <-chan error
type CloseFunc func() Future

type BufferSize int

// Implement this interface for custom encoding/decoding.
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
	//
	// Note: The CloseFunc is asynchronous and returns a Future (asynchronous channel).
	//       The channel should not be used after the CloseFunc is called.
	Send(namespace Namespace, bufferSize BufferSize) (chan<- any, CloseFunc, error)

	// TODO: Support in the future. Use an epidemic broadcast approach.
	// Broadcast(namespace Namespace, ctx context.Context) (chan<- T, context.CancelFunc, error)

	// Receive returns a channel that can be used to receive messages from the distributed channel.
	// Explicitly run CloseFunc to close the channel.
	//
	// If a local channel already exists, the same instance is returned (bufferSize is ignored).
	//
	// Note: The CloseFunc is asynchronous and returns a Future (asynchronous channel)
	//       The channel can be used if items in the buffer. Otherwise check for channel closed.
	Receive(namespace Namespace, bufferSize BufferSize) (<-chan any, CloseFunc, error)

	Close() Future
}

// WithWait creates a new WaitingSend object that can be used to send a message with a wait
// until the message is sent. The context is the time to send a message.
//
// Warning: Using a client wait should be used with caution because it could
// block forever (e.g. servers crash). Set a timeout to avoid blocking forever.
// 
// The object must be sent through the channel
// 
// sending := WithWait(obj)
// 
// chann <- sending
// 
// <- sending.Done()
func WithWait(obj any, ctx context.Context) *WaitingSend {
	ctx, cancel := context.WithCancel(ctx)
	return &WaitingSend{value: obj, ctx: ctx, done: cancel}
}

// channel contains local chan that tracks reference counts
// and a close function.
type channel struct {
	namespace Namespace

	ch       chan any
	refCount int

	closed    bool
	closeFunc CloseFunc
}

// rchannel contains local channel and context for receiving.
// It allows for context cancellation to stop receiving messages.
type rchannel struct {
	channel

	// Senders should increment this count before pushing into
	// the channel. When the count reaches 0 and the context
	// is done, we close the channel.
	sendingCount atomic.Int32

	// Once refCount reaches 0, we want to synchronize with the
	// close goroutine to close the channel.
	//
	// Ensure a buffer of 1 to avoid race condition blocking.
	closeCh chan struct{}
	ctx     context.Context
}

// externalChannel contains information about a server that is receiving this channel.
// protected by the dchan.rcmu lock
type externalChannel struct {
	servers []ServerId

	// goroutines waiting for a receiver to send (synchronous channel)
	//
	// A baton (struct{}) to pass the write lock.
	// e.g. if we have a waiter, we let the waiter unlock the write lock.
	//
	// Instead of using a list.List we don't care about the order of waiters waking up
	// because once one wakes up, we chain them all to wakeup. This allows us to
	// optimize using a RWMutex (Read Lock) instead of a Mutex to allow faster
	// concurrent waiting.
	waitQueue chan struct{}

	// Increment before releasing the read lock. This is a micro optimization
	// while sharing a Read Lock.
	waitCount atomic.Int32
}

// dChan is a distributed channel that can be used to send and receive messages between nodes.
// It is a wrapper around a map of dChanStreams, one for each namespace.
type dChan struct {

	// Servers that are receiving this channel (open for receiving).
	// Used for senders to know which servers to send to.
	// Updated via Raft FSM.
	rcmu             sync.RWMutex
	externalChannels map[Namespace]*externalChannel

	// channels (local sender) that are open for sending from Send(...)
	smu     sync.RWMutex
	senders map[Namespace]*channel

	// channels (local receiver) that are open for receiving from Receive(...)
	rmu       sync.RWMutex
	receivers map[Namespace]*rchannel

	tm   *transport.Manager
	raft *raft.Raft
}

func (d *dChan) Send(namespace Namespace, bufferSize BufferSize) (chan<- any, CloseFunc, error) {
	d.smu.Lock()
	defer d.smu.Unlock()

	if c, ok := d.senders[namespace]; ok {
		return c.ch, c.closeFunc, nil
	}

	chann := makeChannel(namespace, bufferSize)
	chann.closeFunc = func() Future {
		future := d.makeFuture()

		go func() {
			d.smu.Lock()
			defer d.smu.Unlock()

			chann.refCount--
			if chann.refCount > 0 || chann.closed {
				future <- nil
				return
			}

			delete(d.senders, namespace)
			chann.closed = true
			close(chann.ch)
			future <- nil
		}()

		return future
	}

	d.senders[namespace] = chann
	sender := &sender{dchan: d, chann: chann}
	sender.start() // Start the sender goroutine.

	return chann.ch, chann.closeFunc, nil
}

func (d *dChan) Receive(namespace Namespace, bufferSize BufferSize) (<-chan any, CloseFunc, error) {
	d.rmu.Lock()
	defer d.rmu.Unlock()

	if chann, ok := d.receivers[namespace]; ok {
		chann.refCount++
		return chann.ch, chann.closeFunc, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	chann := makeChannel(namespace, bufferSize)
	rchann := &rchannel{channel: *chann, ctx: ctx, closeCh: make(chan struct{})}
	rchann.closeFunc = func() Future {
		future := d.makeFuture()
		go func() {
			d.rmu.Lock()
			rchann.refCount--
			if rchann.refCount > 0 || rchann.closed { // Receiver instances still exist
				future <- nil
				d.rmu.Unlock()
				return
			}

			rchann.closed = true

			// Make sure this instance is deleted
			//
			// Note: atp the current receiver can never be read again since
			// the reference was deleted with the lock.
			delete(d.receivers, namespace)
			d.rmu.Unlock() // Early unlock to not block for waiting senders.

			cancel() // Cancel context to stop receiving messages.

			// Case 1: Senders exist, we wait until they finish and synchronize
			// Case 2: No senders exist OR last sender already cancelled,
			//   we close the channel.
			//
			// Note: we increment sendingCount before releasing read lock,
			// therefore the count can't increase after we acquired write lock
			// and the mapping was deleted.
			if count := rchann.sendingCount.Load(); count > 0 {
				<-rchann.closeCh
			}

			close(chann.ch)
			future <- nil
		}()

		return future
	}

	d.receivers[namespace] = rchann

	// Create a goroutine to receive messages from a gRPC stream.
	// Send Raft message to add Receiver to the namespace.
	return rchann.ch, rchann.closeFunc, nil
}

func (d *dChan) Close() Future {
	d.smu.Lock()
	d.rmu.Lock()
	d.rcmu.Lock()
	defer d.rcmu.Unlock()
	defer d.rmu.Unlock()
	defer d.smu.Unlock()

	future := d.makeFuture()

	// TODO: Close all channels and remove from senders and receivers.
	f := d.raft.Shutdown()
	f.Error()

	return future
}

// makeChannel creates a new channel with the given namespace and buffer size.
// This function is used to create both sender and receiver channels.
func makeChannel(namespace Namespace, bufferSize BufferSize) *channel {
	return &channel{namespace: namespace, ch: make(chan any, bufferSize), refCount: 1, closed: false}
}

// getExternalChannel gets the external channel for the given namespace.
// If it doesn't exist, it creates a new one.
//
// Caller must hold the rcmu lock.
func (d *dChan) getExternalChannel(namespace Namespace) *externalChannel {
	if _, ok := d.externalChannels[namespace]; !ok {
		d.externalChannels[namespace] = &externalChannel{
			servers:   make([]ServerId, 0),
			waitQueue: make(chan struct{}),
		}
	}

	return d.externalChannels[namespace]
}

func (d *dChan) makeFuture() chan error {
	return make(chan error, 1)
}

func newRaft() {
	// https://github.com/Jille/raft-grpc-example/blob/master/main.go
	// https://github.com/hashicorp/raft-boltdb
	// https://github.com/hashicorp/raft
	_ = transport.New(raft.ServerAddress("localhost:1234"), []grpc.DialOption{})

}
