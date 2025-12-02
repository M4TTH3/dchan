# Distributed Go Channels

A fun project to encapsulate various learnings from Distributed Systems (CS454) and Concurrency (CS343) courses.

This project aims to create distributed channels with multiple senders and receivers built on top of Raft, RPC, and a Mailbox system similar to [Actors](https://onlinelibrary.wiley.com/doi/full/10.1002/spe.3262). The interface should be easy to use, feel native, and support high throughput. In creating a native feel, the library is built on [gorpc](https://github.com/valyala/gorpc), a library extending net/rpc, and to simplify startup each server is a node itself. Therefore, a dedicated server administrator is not required to perform message passing.

Current distributed channels are focused on 1 -> 1 connections built on top of RPCs. This however makes it hard to scale, for instance, having multiple receivers or senders. Another option is using a current Actor interface, similar to the mentioned above or Elixir's Actors framework. This implementation takes a step back, implementing the core of the Actor framework, the mailbox with go channels. This makes it feel intuitive and easier to use compared to full Actor frameworks where a whole object must be created. Additionally, one could recreate the Actors simply by passing these to a custom goroutine and object.

A few examples are:
- https://github.com/dradtke/distchan
- https://github.com/billziss-gh/netchan
- https://github.com/asynkron/protoactor-go
- https://github.com/Tochemey/goakt

Main Implementation:
- Built on HashiCorp/Raft, each server/client receiver/sender registers themselves with a namespace (channel name). This allows them all to maintain strong consistency on which channels are active.
- Messages are sent from node->node via gorpc using standard structs
- Broadcasts are sent RPC similarly
- Similar to go channels and unlike Actors, we don't expect a response back. Future consideration may be extended to allow this function, however, it'll expect receiver functions to handle in a consistent way. This branches into the Actor territory, and if that's the goal, it's already built in. Similarly, it may be bad for lazy loading or back pressures, and these channels are meant to be lazily sent to reduce back pressure.



