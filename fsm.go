package dchan

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/hashicorp/raft"
)

// fsm is the state machine for the distributed channel.
// It is responsible for tracking receivers for a namespace.
type fsm struct {
	dchan *dChan
}

var _ raft.BatchingFSM = &fsm{}
var _ raft.FSMSnapshot = &snapshot{}

type fsmCmdType int

const (
	registerReceiver fsmCmdType = iota
	unregisterReceiver
)

type fsmCmd struct {
	Type      fsmCmdType
	Namespace Namespace
	ServerId  ServerId
	Requester ServerId
}

func encodeFsmCmd(cmd fsmCmd) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(cmd); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeFsmCmd(data []byte) (fsmCmd, error) {
	var cmd fsmCmd
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&cmd); err != nil {
		return fsmCmd{}, err
	}
	return cmd, nil
}

// Apply applies a log entry to the state machine.
func (f *fsm) Apply(log *raft.Log) any {
	cmd, err := decodeFsmCmd(log.Data)
	if err != nil {
		return err
	}

	// Only apply commands.
	if log.Type != raft.LogCommand {
		return nil
	}

	// Cases:
	// 1. RegisterReceiver for a namespace not in this node -> success
	// 2. UnregisterReceiver for a namespace not in this node -> success
	// 3. RegisterReceiver for a namespace in this node ->
	//    - if the receiver exists then succeed
	//    - if the receiver does not exist then call a raftUnregisterReceiverCmd.
	//      this occurs when the node is restarted and the receiver was not registered.
	//      safe to call close any time
	// 4. UnregisterReceiver for a namespace in this node -> succeed. Need to make sure to hold rmu and rcmu locks.

	switch cmd.Type {
	case registerReceiver:
		future := f.dchan.fsmRegisterReceiver(cmd.Namespace, cmd.ServerId)
		if err := future.Wait(); err != nil {
			return err
		}
	case unregisterReceiver:
		future := f.dchan.fsmUnregisterReceiver(cmd.Namespace, cmd.ServerId, cmd.Requester)
		if err := future.Wait(); err != nil {
			return err
		}
	}

	return nil
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
func (f *fsm) ApplyBatch(logs []*raft.Log) []any {
	result := make([]any, len(logs))

	return result
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
