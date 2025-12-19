# Distributed Go Channels

A fun project to use various learnings from Distributed Systems (CS454) and Concurrency (CS343) courses.

This project aims to create distributed channels with multiple senders and receivers built on top of Raft, RPC, and a Mailbox system similar to [Actors](https://onlinelibrary.wiley.com/doi/full/10.1002/spe.3262). The interface should be easy to use, feel native, and support high throughput. In creating a native feel, the library is built on grpc with Gob encoding to pass structs directly, and to simplify startup each server is a node itself. Therefore, a dedicated server administrator (like RabbitMQ) is not required to perform message passing.

Similar projects are focused on 1 -> 1 connections built on top of RPCs. This however makes it hard to scale, for instance, having multiple receivers or senders. Another option is using a current Actor interface, similar to the mentioned above or Elixir's Actors framework. Distributed Channels take a step back, implementing the core of the Actor framework, the mailbox, with go channels. This makes it feel intuitive and easier to use compared to full Actor frameworks where a methods must be created. Additionally, one could recreate the Actors simply by passing these to a custom goroutine.

A few examples are:
- https://github.com/dradtke/distchan
- https://github.com/billziss-gh/netchan
- https://github.com/asynkron/protoactor-go
- https://github.com/Tochemey/goakt

Main Implementation:
- Built on HashiCorp/Raft, each server/client receiver/sender registers themselves with a namespace (channel name). This allows them all to maintain strong consistency on which channels are active.
- Messages are sent from node->node via gRPC using standard structs (at most one semantics)
- Broadcasts are sent RPC similarly

TODO:
- [ ] Make the transport layer better.
- [ ] Add connection pool and connection hijacking (e.g. single source of connection management)
- [ ] Add tests for the transport layer (especially more than 2 nodes)
- [ ] Setup interfaces for Distributed Channels
- [ ] Implement the Raft FSM to register receivers and functions to register in and out as receivers
- [ ] Implement the gRPC communications
- [ ] Add tests!!!!

