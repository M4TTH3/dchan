package transport

import (
	"container/list"
	"sync"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type EjectType int

const (
	ROUND_ROBIN EjectType = iota
	LRU
)

type ClientPool interface {
	GetClient(target raft.ServerAddress) (*grpc.ClientConn, error)
	CloseClient(target raft.ServerAddress) error
	Close() error
}

func NewClientPool(maxClients int, ejectType EjectType, dialOptions []grpc.DialOption) ClientPool {
	return &clientPool{
		clients: make(map[raft.ServerAddress]clientConn),
		shadowQueue: list.New(),
		maxClients: maxClients,
		ejectType: ejectType,
		dialOptions: dialOptions,
	}
}

type clientConn struct {
	conn *grpc.ClientConn;
	element *list.Element;
}

type clientPool struct {
	mu sync.Mutex;

	// Map of clients to their connections.
	clients map[raft.ServerAddress]clientConn;

	// Shadow queue of clients (server addresses) to eject.
	shadowQueue *list.List;
	
	maxClients int;
	ejectType EjectType;

	// Dial options to use for new connections.
	dialOptions []grpc.DialOption;
}

func (c *clientPool) GetClient(target raft.ServerAddress) (*grpc.ClientConn, error) {
	c.mu.Lock();
	defer c.mu.Unlock();

	if cconn, ok := c.clients[target]; ok {
		// Move to the front of the queue.
		if c.ejectType == LRU {
			c.shadowQueue.MoveToFront(cconn.element);
		}

		return cconn.conn, nil;
	}

	if c.shadowQueue.Len() >= c.maxClients {
		// The end of the queue is always the one to eject.
		serverAddress := c.shadowQueue.Back().Value.(raft.ServerAddress);
		c.closeClient(serverAddress);
	}

	conn, err := grpc.Dial(string(target), c.dialOptions...)
	if err != nil {
		return nil, err;
	}

	clientConn := clientConn{
		conn: conn,
		element: c.shadowQueue.PushFront(target),
	}
	c.clients[target] = clientConn;

	return conn, nil;
}

// CloseClient closes the client connection for the given target.
// Caller must hold the mutex.
func (c *clientPool) closeClient(target raft.ServerAddress) error {
	if clientConn, ok := c.clients[target]; ok {
		if err := clientConn.conn.Close(); err != nil {
			return err;
		}
		c.shadowQueue.Remove(clientConn.element);
		delete(c.clients, target);
	}

	return nil;
}

// CloseClient closes the client connection for the given target.
func (c *clientPool) CloseClient(target raft.ServerAddress) error {
	c.mu.Lock();
	defer c.mu.Unlock();

	return c.closeClient(target);
}

func (c *clientPool) Close() error {
	c.mu.Lock();
	defer c.mu.Unlock();

	for target, _ := range c.clients {
		c.closeClient(target);
	}

	return nil;
}
