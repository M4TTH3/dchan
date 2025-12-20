// Package transport provides a Transport for github.com/hashicorp/raft over gRPC.
package transport

import (
	"sync"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/m4tth3/dchan/transport/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

var (
	errCloseErr = errors.New("error closing connections")
)

type Manager struct {
	localAddress raft.ServerAddress

	rpcChan          chan raft.RPC
	heartbeatFunc    func(raft.RPC)
	heartbeatFuncMtx sync.Mutex
	heartbeatTimeout time.Duration

	shutdown     bool
	shutdownCh   chan struct{}
	shutdownLock sync.Mutex

	cm ConnectionManager
}

// New creates both components of raft-grpc-transport: a gRPC service and a Raft Transport.
func New(localAddress raft.ServerAddress, dialOptions []grpc.DialOption, options ...Option) *Manager {
	m := &Manager{
		localAddress: localAddress,

		rpcChan:    make(chan raft.RPC),
		shutdownCh: make(chan struct{}),

		cm: NewConnectionManager(dialOptions...),
	}

	for _, opt := range options {
		opt(m)
	}

	return m
}

// Register the RaftTransport gRPC service on a gRPC server.
func (m *Manager) Register(s grpc.ServiceRegistrar) {
	pb.RegisterRaftTransportServer(s, raftServer{manager: m})
}

func (m *Manager) Transport() raft.Transport {
	return &transport{manager: m}
}

func (m *Manager) ConnectionManager() ConnectionManager {
	return m.cm
}

func (m *Manager) Close() error {
	m.shutdownLock.Lock()
	defer m.shutdownLock.Unlock()

	if m.shutdown {
		return nil
	}

	close(m.shutdownCh)
	m.shutdown = true
	return m.cm.DisconnectAll()
}
