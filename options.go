package dchan

import (
	"context"
	"time"

	"google.golang.org/grpc"
)

var (
	DefaultSendTimeout = 2 * time.Minute
	DefaultClusterTimeout = 2 * time.Minute
)

type Option func(*dChan) error

type Config struct {
	// Server ID of this server.
	Id ServerId

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

// WithSendTimeout configures the send timeout.
// Default to 2 minute.
//
// Don't recommend using a very small timeout.
func WithSendTimeout(timeout time.Duration) Option {
	return func(d *dChan) error {
		d.SendTimeout = timeout
		return nil
	}
}

// WithClusterTimeout configures the cluster timeout.
// Default to 2 minute.
//
// Don't recommend using a very small timeout.
func WithClusterTimeout(timeout time.Duration) Option {
	return func(d *dChan) error {
		d.ClusterTimeout = timeout
		return nil
	}
}

// WithConfig configures the dChan with the given config.
func WithConfig(config *Config) Option {
	return func(d *dChan) error {
		d.Config = *config
		return nil
	}
}

// Optionally reuse a gRPC server that exists.
func WithGrpcServer(addr string, server *grpc.Server) Option {
	return func(d *dChan) error {
		d.Id = ServerId(addr)
		d.server = server
		return nil
	}
}

func WithDialOptions(options ...grpc.DialOption) Option {
	return func(d *dChan) error {
		d.DialOptions = options
		return nil
	}
}

func WithCallOptions(options ...grpc.CallOption) Option {
	return func(d *dChan) error {
		d.CallOptions = options
		return nil
	}
}

func WithServerOptions(options ...grpc.ServerOption) Option {
	return func(d *dChan) error {
		d.ServerOptions = options
		return nil
	}
}
