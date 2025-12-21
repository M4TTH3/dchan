package dchan

import (
	"github.com/hashicorp/raft"
	"io"
)

// fsm is the state machine for the distributed channel.
// It is responsible for tracking receivers for a namespace.
type fsm struct {
	dchan *dChan
}

var _ raft.BatchingFSM = &fsm{} // Compile time check
var _ raft.FSMSnapshot = &snapshot{}

// Apply applies a log entry to the state machine.
func (f *fsm) Apply(*raft.Log) interface{} {
	panic("unimplemented")
}

// Restore restores the state machine from a snapshot.
func (f *fsm) Restore(snapshot io.ReadCloser) error {
	panic("unimplemented")
}

// Snapshot creates a snapshot of the state machine.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	panic("unimplemented")
}

// ApplyBatch applies a batch of log entries to the state machine.
func (f *fsm) ApplyBatch([]*raft.Log) []interface{} {
	panic("unimplemented")
}

type snapshot struct {
}

// Persist persists the snapshot to the sink.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	panic("unimplemented")
}

// Release releases the snapshot.
func (s *snapshot) Release() {
	panic("unimplemented")
}
