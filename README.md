# dchan - Distributed Go Channels

A Go library that extends native channel semantics across a cluster of nodes. Each server is a peer -- no dedicated broker or administrator is required. Inspired by learnings from Distributed Systems (CS454) and Concurrency (CS343).

## Quick Start

```go
import "github.com/m4tth3/dchan"

// Every node creates a dchan instance with the same cluster info.
ch, _ := dchan.New(myAddr, "cluster-1", peerAddrs, "./data")
defer func() { ch.Close().Wait() }()

// Receive on a namespace (any node)
recvCh, closeRecv, _ := ch.Receive("orders", 10)
defer func() { closeRecv().Wait() }()
order := <-recvCh

// Send on the same namespace (any node)
sendCh, closeSend, _ := ch.Send("orders", 10)
defer func() { closeSend().Wait() }()
sendCh <- Order{ID: 1, Item: "widget"}

// Optionally track delivery with WithMessage
msg := dchan.WithMessage(Order{ID: 2}, 30*time.Second)
sendCh <- msg
if msg.Done() {
    // guaranteed received by a peer
}
```

> Types sent through channels must be registered with `gob.Register(MyType{})` so they can be encoded/decoded through `any`.

## Architecture

dchan splits coordination and data transfer into two separate planes that share the same gRPC connections:

```
                        Raft Consensus (Control Plane)
                 ┌──────────────────────────────────────┐
                 │  Tracks which nodes receive which     │
                 │  namespaces. Applied via FSM.         │
                 │                                       │
                 │   Leader ◄──► Follower ◄──► Follower  │
                 └──────────────────────────────────────┘

                        gRPC (Data Plane)
                 ┌──────────────────────────────────────┐
                 │  Messages sent peer-to-peer.          │
                 │  Gob-encoded, round-robin across      │
                 │  receivers. Supports backpressure.     │
                 │                                       │
                 │   Node A ────message────► Node B      │
                 │   Node A ────message────► Node C      │
                 └──────────────────────────────────────┘
```

### Send Flow

```
 ch <- value
    │
    ▼
 sender goroutine
    │
    ├─ 1. waitForTarget()      ◄── reads receiver set (populated by Raft FSM)
    │     (blocks if no receivers)
    │
    ├─ 2. gobEncode(value)
    │
    ├─ 3. gRPC Receive(data)   ──► target node pushes into local channel
    │     (round-robin, retry on reject)
    │
    └─ 4. done / next message
```

### Receiver Registration Flow

```
 ch.Receive("ns", buf)
    │
    ├─ 1. Create local channel
    │
    ├─ 2. RegisterReceiver RPC ──► Raft leader
    │                                  │
    │                                  ▼
    │                           raft.Apply(cmd)
    │                                  │
    │                                  ▼
    │                           FSM adds this node to
    │                           receiver set for "ns"
    │                           (replicated to all nodes)
    │
    └─ 3. Return <-chan any to caller
```

## Implementation Details

- **Raft** (HashiCorp/Raft) provides sequentially consistent coordination of receiver state: which nodes are listening on which namespaces.
- **gRPC** carries actual messages directly between sender and receiver nodes, bypassing the Raft log entirely.
- **Shared connections**: the Raft transport and dchan message RPCs reuse the same gRPC server and client connections.
- **Gob encoding** for messages. Users can send any Go type (including structs) registered with `gob.Register`.
- **At-most-once semantics**: a message is delivered to exactly one receiver or not at all.
- **Backpressure**: if a receiver's channel buffer is full, the sender blocks (or times out), naturally rate-limiting producers.
- **Smart-client round-robin**: senders distribute messages across available receivers with a randomized starting offset.

### Pros and Cons

| Pros | Cons |
|------|------|
| No dedicated broker -- every node is a peer | Raft leader is a bottleneck for receiver state changes (register/unregister) |
| Messages travel directly sender-to-receiver (low latency) | Messages are not persisted -- lost if the receiver crashes mid-flight |
| Native Go channel interface (`chan<- any`, `<-chan any`) | At-most-once delivery; no built-in retries at the application level |
| Raft ensures all nodes agree on who is receiving | Every node is a Raft voter, adding consensus overhead as the cluster grows |
| Connection reuse between Raft and messaging reduces overhead | Cluster bootstrap requires a known set of initial addresses |
| Built-in backpressure from channel semantics | Gob encoding requires type registration for custom types |

## Similar Projects

| Project | Limitation |
|---------|-----------|
| [distchan](https://github.com/dradtke/distchan) | One connection per channel, single receiver |
| [netchan](https://github.com/billziss-gh/netchan) | Point-to-point only, no cluster coordination |
| [protoactor-go](https://github.com/asynkron/protoactor-go) | Full actor framework, heavy boilerplate |
| [goakt](https://github.com/Tochemey/goakt) | Full actor framework, requires separate discovery |

dchan focuses on the mailbox primitive at the core of actor systems -- distributed channels -- without the framework overhead.

## TODO

- [x] Transport layer with connection reuse
- [x] Raft FSM for receiver coordination
- [x] gRPC message passing with backpressure
- [x] Integration tests (multi-node send/receive)
- [ ] Epidemic broadcast (`Broadcast` API)
- [ ] Chunked transfer for large messages
