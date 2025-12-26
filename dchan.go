package dchan

import (
	"context"
	"encoding/gob"
	"errors"
	"maps"
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

type CloseFunc func() Future

type BufferSize int

// Implement this interface for custom encoding/decoding.
type CustomEncodable interface {
	gob.Encoder
	gob.Decoder
}

// Chan is the interface for a distributed channel that can be used to send and receive messages between nodes.
// Note: it only works with exported fields (unexported fields are ignored).
//
// Future optimization:
// - Start as a non-voter and upgrade to a voter if we start receiving messages. Idk if this is worth it tbh.
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

// New creates a new dChan with the given address, session ID, cluster addresses, and options.
// The address is the address of this server.
// The session ID is the session ID of this server.
// The cluster addresses are the addresses of initial cluster members (e.g. possibly itself).
// The options are the options for the dChan.
func New(addr string, sessionId string, clusterAddr []string, options ...Option) (Chan, error) {
	server := grpc.NewServer()

	d := &dChan{
		server: server,
	}

	for _, option := range options {
		if err := option(d); err != nil {
			return nil, err
		}
	}

	return d, nil
}

// channel contains local chan that tracks reference counts
// and a close function.
type channel struct {
	namespace Namespace

	ch       chan any
	refCount int

	// Once all the tasks (sender/receiver) the closeFunc synchronizes with
	// finish, we can return the future result.
	//
	// e.g. wait for the sender to finish (send across gRPC or timeout) OR
	// all receivers that are trying to push into the channel finish
	//
	// Ensure a buffer of size 1 to avoid race condition blocking.
	closeCh chan struct{}

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

	// Cond to wait for an rchannel to be deleted. Once deleted,
	// this cond will be broadcasted.
	//
	// Caller must hold the rmu lock.
	delCond *sync.Cond

	ctx context.Context
}

// externalChannel contains information about a server that is receiving this channel.
// protected by the dchan.rcmu lock
type externalChannel struct {
	servers *orderedSet[ServerId]

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
	Config

	// Servers that are receiving this namespace (open for receiving).
	// Used for senders to know which servers to send to (round-robin).
	// Updated via Raft FSM.
	extmu            sync.RWMutex
	externalChannels map[Namespace]*externalChannel

	// channels (local sender) that are open for sending from Send(...)
	smu     sync.RWMutex
	senders map[Namespace]*channel

	// channels (local receiver) that are open for receiving from Receive(...)
	rmu       sync.RWMutex
	receivers map[Namespace]*rchannel

	tm     *transport.Manager
	server *grpc.Server
	raft   *raft.Raft
}

var _ Chan = &dChan{}

func (d *dChan) Send(namespace Namespace, bufferSize BufferSize) (chan<- any, CloseFunc, error) {
	d.smu.Lock()
	defer d.smu.Unlock()

	if c, ok := d.senders[namespace]; ok {
		return c.ch, c.closeFunc, nil
	}

	chann := newChannel(namespace, bufferSize)
	chann.closeFunc = d.newSendCloseFunc(chann, namespace)

	d.senders[namespace] = chann
	sender := &sender{dchan: d, chann: chann}
	sender.start() // Start the sender goroutine.

	return chann.ch, chann.closeFunc, nil
}

func (d *dChan) Receive(namespace Namespace, bufferSize BufferSize) (<-chan any, CloseFunc, error) {
	d.rmu.Lock()
	defer d.rmu.Unlock()

	if chann, ok := d.receivers[namespace]; ok {
		if chann.closed {
			chann.delCond.Wait()
		} else {
			chann.refCount++
			return chann.ch, chann.closeFunc, nil
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	rchann := &rchannel{channel: *newChannel(namespace, bufferSize), ctx: ctx, delCond: sync.NewCond(&d.rmu)}
	rchann.closeFunc = d.newReceiveCloseFunc(rchann, cancel, namespace)

	d.receivers[namespace] = rchann

	if err := d.registerReceiver(namespace).Wait(); err != nil {
		return nil, nil, err
	}

	// Create a goroutine to receive messages from a gRPC stream.
	// Send Raft message to add Receiver to the namespace.
	return rchann.ch, rchann.closeFunc, nil
}

func (d *dChan) Close() Future {
	d.smu.Lock()
	d.rmu.Lock()
	d.extmu.Lock()
	defer d.extmu.Unlock()
	defer d.rmu.Unlock()
	defer d.smu.Unlock()

	future := newFuture()

	// TODO: Close all channels and remove from senders and receivers.
	f := d.raft.Shutdown()
	f.Error()

	return future
}

// newSendCloseFunc creates a new close function for a sender channel.
// The close function synchronizes with the sender goroutine to close
// the channel
func (d *dChan) newSendCloseFunc(chann *channel, namespace Namespace) CloseFunc {
	return func() Future {
		future := newFuture()

		go func() {
			d.smu.Lock()
			chann.refCount--
			if chann.refCount > 0 || chann.closed {
				future.set(nil)
				d.smu.Unlock()
				return
			}

			chann.closed = true
			delete(d.senders, namespace)
			d.smu.Unlock() // Early unlock to not block for waiting receivers.

			close(chann.ch) // notify the sender to finish

			<-chann.closeCh // wait for it to finish
			future.set(nil)
		}()

		return future
	}
}

// newReceiveCloseFunc creates a new close function for a receiver channel.
// The close function synchronizes with the server.Receive goroutines to close
// the channel
//
// We recommend waiting on the future before re-opening in the same namespace.
// Otherwise, it could be magnitude slower as we have to fix the logs.
func (d *dChan) newReceiveCloseFunc(rchann *rchannel, cancel context.CancelFunc, namespace Namespace) CloseFunc {
	return func() Future {
		future := newFuture()
		go func() {
			d.rmu.Lock()
			rchann.refCount--
			if rchann.refCount > 0 || rchann.closed { // Receiver instances still exist
				future.set(nil)
				d.rmu.Unlock()
				return
			}

			// Signal flag to new registers to this namespace to wait.
			// Note: any messages still received will now return received: false
			// unless someone re-registers but it'll be a different channel
			rchann.closed = true
			d.rmu.Unlock() // Early unlock to not block for waiting senders.

			// Even if there's a race e.g. we register again before we unregister,
			// the local FSM will push another registerCommand to fix
			unregisterFuture := d.unregisterReceiver(namespace, d.Id)
			if err := unregisterFuture.Wait(); err != nil {
				future.set(err) // Maybe we should retry? Close should be idempotent.
				return
			}

			// Cancel context to stop receiving messages this this channel.
			// Placed after the unregister to give more time to receivers.
			cancel()

			// Two steps to not block other independent receivers from reigstering.
			// New registers to this namespace will be blocked until this finishes.
			d.rmu.Lock()
			delete(d.receivers, namespace) // This instance is no longer valid.
			rchann.delCond.Broadcast()     // Notify any waiters that the channel is deleted.
			d.rmu.Unlock()

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

			close(rchann.ch)
			future.set(nil)
		}()

		return future
	}
}

// newChannel creates a new channel with the given namespace and buffer size.
//
// This function is used to for both sender and receiver channels.
// Caller must set closeFunc otherwise it's nil
func newChannel(namespace Namespace, bufferSize BufferSize) *channel {
	return &channel{
		namespace: namespace,
		ch:        make(chan any, bufferSize),
		refCount:  1,
		closeCh:   make(chan struct{}, 1),
		closed:    false,
	}
}

// getExternalChannel gets the external channel for the given namespace.
// If it doesn't exist, it creates a new one.
//
// Caller must hold the rcmu lock.
func (d *dChan) getExternalChannel(namespace Namespace) *externalChannel {
	if _, ok := d.externalChannels[namespace]; !ok {
		d.externalChannels[namespace] = &externalChannel{
			servers:   newOrderedSet[ServerId](),
			waitQueue: make(chan struct{}),
		}
	}

	return d.externalChannels[namespace]
}

// Called from the FSM to register a receiver for a namespace.
func (d *dChan) fsmRegisterReceiver(namespace Namespace, serverId ServerId) Future {
	d.rmu.RLock() // Unlock after extmu or early return

	// If the receiver exists then it's a local call or double registration.
	// We can ignore it and safely return.
	//
	// Otherwise, we can assume it's an Apply after a restart
	// and the receiver was not registered.
	//
	// We should send an unregisterReceiver command to clean up the receiver.
	if serverId == d.Id {
		_, ok := d.receivers[namespace]
		if !ok {
			d.rmu.RUnlock()
			return d.unregisterReceiver(namespace, serverId)
		}
	}

	future := newFuture()

	// Acquire the write lock to update externalChannel.servers
	d.extmu.Lock()
	d.rmu.RUnlock()

	externalChannel := d.getExternalChannel(namespace)
	externalChannel.servers.put(serverId)

	// Pass the baton if plausible
	if externalChannel.waitCount.Load() > 0 {
		externalChannel.waitQueue <- struct{}{}
	} else {
		d.extmu.Unlock()
	}

	future.set(nil)
	return future
}

// Called from the FSM to unregister a receiver for a namespace.
func (d *dChan) fsmUnregisterReceiver(namespace Namespace, serverId ServerId, sender ServerId) Future {
	d.rmu.RLock() // Unlock after extmu or early return

	// call raftUnregisterReceiverCmd to unregister the receiver.
	// Note: if the close is for this node, requested by another node
	// (e.g. failed to send) then we should actually re-register the
	// receiver if it's still up
	if serverId == d.Id && sender != serverId {
		if receiver, ok := d.receivers[namespace]; ok && !receiver.closed {
			d.rmu.RUnlock()
			return d.registerReceiver(namespace)
		}
	}

	d.extmu.Lock()
	d.rmu.RUnlock() // TODO can we unlock early?
	defer d.extmu.Unlock()

	future := newFuture()

	externalChannel := d.getExternalChannel(namespace)
	externalChannel.servers.delete(serverId)

	future.set(nil)
	return future
}

// Called from the FSM to get the current state of the external channels.
// This is used to create a snapshot of the state machine.
func (d *dChan) fsmGetState() map[Namespace][]ServerId {
	d.extmu.RLock()
	defer d.extmu.RUnlock()

	state := make(map[Namespace][]ServerId)
	for namespace, externalChannel := range d.externalChannels {
		state[namespace] = externalChannel.servers.toSlice()
	}

	return state
}

// Called from the FSM to restore the state of the external channels.
// This is used to restore the state machine from a snapshot.
//
// This function should match receiver state with the saved state of the server
// when this server is in the list.
//
// Case 1: receiver exists (and not closed) and that namespace isn't in external channel.
//
//	then register the receiver. We can ignore this as the snapshot is behind.
//
// Case 2: namespace is in external channel but receiver doesn't exist,
//
//	then unregister the receiver.
//
// We should be modifying externalChannels rather than replacing if it exists already.
//
// Note: unregister is idempotent e.g. double unregister is fine but is slow.
func (d *dChan) fsmRestore(state map[Namespace][]ServerId) error {
	d.rmu.RLock()
	defer d.rmu.RUnlock()

	d.extmu.Lock()
	defer d.extmu.Unlock()

	// We're preferring to modify existing entries. So any namespace not in the saved state
	// should be deleted.
	savedKeys := newOrderedSetFromSeq(maps.Keys(state))
	maps.DeleteFunc(d.externalChannels, func(namespace Namespace, externalChannel *externalChannel) bool {
		return !savedKeys.has(namespace)
	})

	// Update the servers in the external channel.
	for namespace, servers := range state {
		externalChannel := d.getExternalChannel(namespace)
		externalChannel.servers = newOrderedSetFromSlice(servers)

		// Proceed if there's no local receiver check
		if !externalChannel.servers.has(d.Id) {
			continue
		}

		// If the local receiver is not open, we should unregister it.
		if _, ok := d.receivers[namespace]; !ok {
			if err := d.unregisterReceiver(namespace, d.Id).Wait(); err != nil {
				return err
			}
		}
	}

	return nil
}

// Send a RegisterReceiver command to the Raft cluster.
// to register a receiver for a namespace in this node.
//
// Note: the future returns the error of raft.Apply e.g.
// ErrNotLeader, ErrLeadershipLost, etc.
// or the error of the gRPC call.
func (d *dChan) registerReceiver(namespace Namespace) Future {
	future := newFuture()

	go func() {
		client, err := d.getLeaderClient()
		if err != nil {
			future.set(err)
			return
		}

		ctx, cancel := d.clusterCtx()
		defer cancel()
		_, err = client.RegisterReceiver(ctx, &p.ReceiverRequest{
			Namespace: string(namespace),
			ServerId:  string(d.Id),
			Requester: string(d.Id),
		})

		future.set(err)
	}()

	return future
}

// Send a UnregisterReceiver command to the Raft cluster.
// to unregister a receiver for a namespace in this node.
//
// Note: the future returns the error of raft.Apply e.g.
// ErrNotLeader, ErrLeadershipLost, etc.
// or the error of the gRPC call.
func (d *dChan) unregisterReceiver(namespace Namespace, serverId ServerId) Future {
	future := newFuture()

	go func() {
		client, err := d.getLeaderClient()
		if err != nil {
			future.set(err)
			return
		}

		ctx, cancel := d.clusterCtx()
		defer cancel()
		_, err = client.UnregisterReceiver(ctx, &p.ReceiverRequest{
			Namespace: string(namespace),
			ServerId:  string(serverId),
			Requester: string(d.Id),
		})

		future.set(err)
	}()

	return future
}

// Send a AddVoter command to the Raft cluster.
// to add a voter to the cluster.
//
// Note: the future returns the error of raft.Apply e.g.
// ErrNotLeader, ErrLeadershipLost, etc.
// or the error of the gRPC call.
func (d *dChan) registerAsVoter() Future {
	future := newFuture()

	go func() {
		client, err := d.getLeaderClient()
		if err != nil {
			future.set(err)
			return
		}

		ctx, cancel := d.clusterCtx()
		defer cancel()
		_, err = client.AddVoter(ctx, &p.ServerInfo{
			IdAddress: string(d.Id),
		})

		future.set(err)
	}()

	return future
}

func (d *dChan) unregisterAsVoter() Future {
	future := newFuture()

	go func() {
		client, err := d.getLeaderClient()
		if err != nil {
			future.set(err)
			return
		}

		ctx, cancel := d.clusterCtx()
		defer cancel()
		_, err = client.RemoveVoter(ctx, &p.ServerInfo{
			IdAddress: string(d.Id),
		})

		future.set(err)
	}()

	return future
}

func (d *dChan) getLeaderClient() (p.DChanServiceClient, error) {
	leader := d.raft.Leader()
	conn, err := d.tm.ConnectionManager().Register(raft.ServerAddress(leader))
	if err != nil {
		return nil, err
	}

	return p.NewDChanServiceClient(conn), nil
}

func NewRaft() {
	// https://github.com/Jille/raft-grpc-example/blob/master/main.go
	// https://github.com/hashicorp/raft-boltdb
	// https://github.com/hashicorp/raft
	_ = transport.New(raft.ServerAddress("localhost:1234"), []grpc.DialOption{})

	// TODO consider adding explicit timeout to each channel message.
	// e.g. a failed node could block forever.
	// Also consider closing that receiver if the node is unresponsive (after sending)
}
