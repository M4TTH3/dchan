package dchan

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	DefaultSendTimeout    = 2 * time.Minute
	DefaultClusterTimeout = 2 * time.Minute
	DefaultSnapshotCount  = 3
	DefaultMaxJoinRetries = 5
	DefaultRetryDelay     = 3 * time.Second

	DefaultCallOptions = []grpc.CallOption{
		grpc.WaitForReady(true),
	}

	DefaultDialOptions = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	DefaultServerOptions = []grpc.ServerOption{
		grpc.Creds(insecure.NewCredentials()),
	}
)

type Option func(*Chan) error

type Config struct {
	// Server ID of this server.
	Id ServerId

	// Base directory for the store.
	StoreDir string

	// Every node must have this unique cluster ID.
	//
	// Currently used for file store names, but possibly we should
	// append it to the address as an ID.
	ClusterId string

	// Cluster addresses of initial cluster members (e.g. possibly itself).
	ClusterAddresses []string

	// Timeout for sending messages
	// Default to 2 minute
	//
	// For large messages, suggest setting a larger timeout OR
	// using a WithTimeout message.
	SendTimeout time.Duration

	// Timeout for cluster operations
	// Default to 2 minute
	ClusterTimeout time.Duration

	// Dial options for the gRPC clients to connect to other servers.
	DialOptions []grpc.DialOption

	// We should have WaitForReady option for the gRPC clients to connect to other servers.
	// Used for messages and cluster operations.
	CallOptions []grpc.CallOption

	// Server options for the gRPC server.
	ServerOptions []grpc.ServerOption

	// Snapshot count for the raft snapshot store.
	SnapshotCount int

	// Max number of retries to join the cluster.
	MaxJoinRetries int

	// Delay between retries.
	RetryDelay time.Duration
}

// sendCtx returns a context with the send timeout.
func (c *Config) sendCtx() (context.Context, context.CancelFunc) {
	if c.SendTimeout > 0 {
		return context.WithTimeout(context.Background(), c.SendTimeout)
	}

	return context.Background(), func() {}
}

// clusterCtx returns a context with the cluster timeout.
func (c *Config) clusterCtx() (context.Context, context.CancelFunc) {
	if c.ClusterTimeout > 0 {
		return context.WithTimeout(context.Background(), c.ClusterTimeout)
	}

	return context.Background(), func() {}
}

func DefaultConfig() Config {
	return Config{
		SendTimeout:    DefaultSendTimeout,
		ClusterTimeout: DefaultClusterTimeout,
		SnapshotCount:  DefaultSnapshotCount,
		MaxJoinRetries: DefaultMaxJoinRetries,
		RetryDelay:     DefaultRetryDelay,

		DialOptions:    DefaultDialOptions,
		CallOptions:    DefaultCallOptions,
		ServerOptions:  DefaultServerOptions,
	}
}

// WithSendTimeout configures the send timeout.
// Default to 2 minute.
//
// Don't recommend using a very small timeout.
func WithSendTimeout(timeout time.Duration) Option {
	return func(d *Chan) error {
		d.SendTimeout = timeout
		return nil
	}
}

// WithClusterTimeout configures the cluster timeout.
// Default to 2 minute.
//
// Don't recommend using a very small timeout.
func WithClusterTimeout(timeout time.Duration) Option {
	return func(d *Chan) error {
		d.ClusterTimeout = timeout
		return nil
	}
}

// WithConfig configures the dChan with the given config.
func WithConfig(config *Config) Option {
	return func(d *Chan) error {
		d.Config = *config
		return nil
	}
}

// Optionally reuse a gRPC server that exists.
func WithGrpcServer(addr string, server *grpc.Server) Option {
	return func(d *Chan) error {
		d.Id = ServerId(addr)
		d.grpcServer = server
		return nil
	}
}

func WithDialOptions(options ...grpc.DialOption) Option {
	return func(d *Chan) error {
		d.DialOptions = options
		return nil
	}
}

func WithCallOptions(options ...grpc.CallOption) Option {
	return func(d *Chan) error {
		d.CallOptions = options
		return nil
	}
}

func WithServerOptions(options ...grpc.ServerOption) Option {
	return func(d *Chan) error {
		d.ServerOptions = options
		return nil
	}
}

func WithSnapshotCount(count int) Option {
	return func(d *Chan) error {
		d.SnapshotCount = count
		return nil
	}
}
