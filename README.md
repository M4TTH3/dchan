# Distributed Go Channels

A fun project to use various learnings from Distributed Systems (CS454) and Concurrency (CS343) courses.

This project aims to create distributed channels with multiple senders and receivers built on top of Raft, RPC, and a Mailbox system similar to [Actors](https://onlinelibrary.wiley.com/doi/full/10.1002/spe.3262). The interface should be easy to use, feel native, and support high throughput. 

For a native feel, the library is built on gRPC with Gob encoding to pass structs, and to simplify startup each server is a node. Therefore, a dedicated server administrator (like RabbitMQ) is not required to perform message passing.

## Usage Example
```go
// Server 1
func main() {
    options := []Options {
        // Other Server IDs or at least one for discovery
        // BoltDB path (file path)
        // Register for Gob encodings
    }

    chann := dchan.New(..., options)

    // <-chan any, CloseFunc, error 
    fooCh, closeFoo, err = chann.Receive("FooChannel", bufSize)
    defer closeFoo()

    item <- fooCh // receive from a sender

    // chan<- any, CloseFunc, error
    oofCh, closeOof, err = chann.Send("OofChannel", bufSize)
    defer closeOof()

    // Send the message to one of the receivers in cluster
    // Note: this could block indefinitely if no receivers and bufSize = 0
    oofCh <- oof{}

    // Optionally use:
    timeout := time.Minute * 2 // really large message oof
    oofObj := dchan.WithMessage(oof{}, timeout)

    select{
    case oofCh <- oof{}: // could block if buffer full
    case oofObj.Done(): // cancel if timeout finishes first
    }

    // sent (bool) determines if we guaranteed know it's reached another channel
    // otherwise it could be in any state e.g. received or not
    sent := oofObj.Done()
}

// Server 2
func main() {
    ...
    // Sending to server 1 (or any other servers registered)
    fooCh <- foo{}

    // Receiving from server 1 (or any other servers registered)
    fmt.Println((<-oofObj).(oof).String())
}

```

## Implementation Details

Main Implementation:
- Built on HashiCorp/Raft and gRPC
- Raft to coordinate (sequential consistency) among nodes whose receiving what namespaces
- gRPC (with Raft connection reuse) to send messages node->node (supporting backpressure and async messaging)
- At most once semantics
- Use gob/encoding for messages, and users can define their own encoders simply
- Broadcasts are sent via an epidemic algorithm over gRPC (TODO)

## Similar Projects

Similar projects are very limited or hard-to-use:
- Each channel creates a new connection (slow) 
- Can only send to one server (bad for distribution) and each receiver requires its own server
- Actor system implementations require too much boiler-plate and declarations. It's a framework in itself.
- Require dedicated instances to distribute messages (MOM), however, it's functionally a bad version of as a service like RabbitMQ

A few examples are:
- https://github.com/dradtke/distchan
- https://github.com/billziss-gh/netchan
- https://github.com/asynkron/protoactor-go
- https://github.com/Tochemey/goakt

This implementation focuses on the core of Actors, aka the Mailbox, which happens to share a similar philosophy to Go Channels. With minimal work, we could recreate an actor system.

## TODO

TODO:
- [x] Make the transport layer better.
- [x] Add connection manager and connection hijacking (e.g. shared between Raft and Messangers)
- [x] Add tests for the transport layer (especially more than 2 nodes)
- [x] Setup interfaces for Distributed Channels
- [x] Implement the Raft FSM to register receivers and functions to register in and out as receivers
- [x] Implement the gRPC communications
- [ ] Add tests!!!!
- [ ] Implement Broadcast (but this can be done later)

