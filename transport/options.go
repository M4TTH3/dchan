package transport

import "time"

type Option func(m *Manager)

// WithHeartbeatTimeout configures the transport to not wait for more than d
// for a heartbeat to be executed by a remote peer.
func WithHeartbeatTimeout(d time.Duration) Option {
	return func(m *Manager) {
		m.heartbeatTimeout = d
	}
}

func WithStreamTimeout(d time.Duration) Option {
	return func(m *Manager) {
		m.streamTimeout = d
	}
}

func WithSendTimeout(d time.Duration) Option {
	return func(m *Manager) {
		m.sendTimeout = d
	}
}