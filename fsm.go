package dchan

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/hashicorp/raft"
)

type fsmManager interface {
	fsmRegisterReceiver(namespace Namespace, serverId ServerID) Future
	fsmUnregisterReceiver(namespace Namespace, serverId ServerID, requester ServerID) Future
	fsmRestore(saved map[Namespace][]ServerID) error
	fsmGetState() map[Namespace][]ServerID
}

// fsm is the state machine for the distributed channel.
// It is responsible for tracking receivers for a namespace.
type fsm struct {
	fm fsmManager
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
	ServerId  ServerID
	Requester ServerID
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

	switch cmd.Type {
	case registerReceiver:
		future := f.fm.fsmRegisterReceiver(cmd.Namespace, cmd.ServerId)
		if err := future.Wait(); err != nil {
			return err
		}
	case unregisterReceiver:
		future := f.fm.fsmUnregisterReceiver(cmd.Namespace, cmd.ServerId, cmd.Requester)
		if err := future.Wait(); err != nil {
			return err
		}
	}

	return nil
}

// Restore restores the state machine from a snapshot.
func (f *fsm) Restore(snapshot io.ReadCloser) error {
	defer snapshot.Close()

	var saved map[Namespace][]ServerID
	dec := gob.NewDecoder(snapshot)
	if err := dec.Decode(&saved); err != nil {
		return err
	}

	return f.fm.fsmRestore(saved)
}

// Snapshot creates a snapshot of the state machine.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{saved: f.fm.fsmGetState()}, nil
}

// ApplyBatch applies a batch of log entries to the state machine.
func (f *fsm) ApplyBatch(logs []*raft.Log) []any {
	result := make([]any, len(logs))

	for i, log := range logs {
		// We could probably optimize this by having one lock,
		// but it's not worth the complexity.
		result[i] = f.Apply(log)
	}

	return result
}

// A snapshot should contain a saved state of the
// external channels only. Specifically, the servers IDs
//
// We have to match the snapshot with the current state of the FSM
// so creator has to block Apply e.g. created in Snapshot()
type snapshot struct {
	saved map[Namespace][]ServerID
}

// Persist persists the snapshot to the sink.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(s.saved); err != nil {
		return err
	}

	if _, err := sink.Write(buf.Bytes()); err != nil {
		return err
	}

	return nil
}

// Release releases the snapshot.
func (s *snapshot) Release() {
	// no-op
}
