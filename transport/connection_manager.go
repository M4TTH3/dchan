package transport

import (
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
)

type ConnectionManager interface {
	// Hijack returns a client connection for the given target.
	// If the connection is not found, it returns nil and false.
	Hijack(target raft.ServerAddress) (*grpc.ClientConn, bool)

	// Register registers a new client connection for the given target.
	// If the connection already exists, it returns the existing connection.
	Register(target raft.ServerAddress) (*grpc.ClientConn, error)

	// Disconnect disconnects the client connection for the given target.
	Disconnect(target raft.ServerAddress) error

	// Close closes all the connections and releases all the resources.
	DisconnectAll() error
}

// Compile time check that clientPool implements ClientPool.
var _ ConnectionManager = &connectionManager{}

func NewConnectionManager(dialOptions ...grpc.DialOption) ConnectionManager {
	mu := &sync.Mutex{}
	return &connectionManager{
		mu:          mu,
		connections: make(map[raft.ServerAddress]*grpc.ClientConn),
		dialOptions: dialOptions,
	}
}

type connectionManager struct {
	mu *sync.Mutex

	// Map of clients to their connections.
	connections map[raft.ServerAddress]*grpc.ClientConn

	// Dial options to use for new connections.
	dialOptions []grpc.DialOption
}

func (c *connectionManager) Hijack(target raft.ServerAddress) (*grpc.ClientConn, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, ok := c.connections[target]
	return conn, ok
}

func (c *connectionManager) Register(target raft.ServerAddress) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.connections[target]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(string(target), c.dialOptions...)
	if err != nil {
		return nil, err
	}

	c.connections[target] = conn

	return conn, nil
}

// closeClientHelper closes the client connection for the given target.
// Caller must hold the mutex.
func (c *connectionManager) disconnect(target raft.ServerAddress) error {
	if conn, ok := c.connections[target]; ok {
		if err := conn.Close(); err != nil {
			return err
		}

		delete(c.connections, target)
	}

	return nil
}

func (c *connectionManager) Disconnect(target raft.ServerAddress) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.disconnect(target)
}

func (c *connectionManager) DisconnectAll() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error = nil
	for target := range c.connections {
		if tmpErr := c.disconnect(target); tmpErr != nil {
			err = multierror.Append(err, tmpErr)
		}
	}

	if err != nil {
		return multierror.Append(errCloseErr, err)
	}

	return nil
}
