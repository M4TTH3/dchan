package dchan

import (
	"github.com/hashicorp/raft"
	p "github.com/m4tth3/dchan/proto"
	"github.com/m4tth3/dchan/transport"
)

// Operations to invoke to the leader of a cluster

type client struct {
	localID string
	raft *raft.Raft
	config *Config
	
	cm transport.ConnectionManager
}

// Send a RegisterReceiver command to the Raft cluster.
// to register a receiver for a namespace in this node.
//
// Note: the future returns the error of raft.Apply e.g.
// ErrNotLeader, ErrLeadershipLost, etc.
// or the error of the gRPC call.
func (c *client) registerReceiver(namespace Namespace) Future {
	future := newFuture()

	go func() {
		client, err := c.getLeaderClient()
		if err != nil {
			future.set(err)
			return
		}

		ctx, cancel := c.config.clusterCtx()
		defer cancel()
		_, err = client.RegisterReceiver(ctx, &p.ReceiverRequest{
			Namespace: string(namespace),
			ServerId:  c.localID,
			Requester: c.localID,
		})

		future.set(err)
	}()

	return future
}

// Send a UnregisterReceiver command to the Raft cluster.
// to unregister a receiver for a namespace in this node.
//
// Note: the future returns the error of raft.Apply e.g.
// ErrNotLeader, ErrLeadershipLost, etc.
// or the error of the gRPC call.
func (c *client) unregisterReceiver(namespace Namespace, serverId ServerId) Future {
	future := newFuture()

	go func() {
		client, err := c.getLeaderClient()
		if err != nil {
			future.set(err)
			return
		}

		ctx, cancel := c.config.clusterCtx()
		defer cancel()
		_, err = client.UnregisterReceiver(ctx, &p.ReceiverRequest{
			Namespace: string(namespace),
			ServerId:  string(serverId),
			Requester: c.localID,
		})

		future.set(err)
	}()

	return future
}

// Send a AddVoter command to the Raft cluster.
// to add a voter to the cluster.
//
// Note: the future returns the error of raft.Apply e.g.
// ErrNotLeader, ErrLeadershipLost, etc.
// or the error of the gRPC call.
func (c *client) registerAsVoter(askAddress string) Future {
	future := newFuture()

	go func() {
		client, err := c.getClient(raft.ServerAddress(askAddress))
		if err != nil {
			future.set(err)
			return
		}

		ctx, cancel := c.config.clusterCtx()
		defer cancel()
		_, err = client.AddVoter(ctx, &p.ServerInfo{
			IdAddress: c.localID,
		})

		future.set(err)
	}()

	return future
}

func (c *client) unregisterAsVoter() Future {
	future := newFuture()

	go func() {
		client, err := c.getLeaderClient()
		if err != nil {
			future.set(err)
			return
		}

		ctx, cancel := c.config.clusterCtx()
		defer cancel()
		_, err = client.RemoveVoter(ctx, &p.ServerInfo{
			IdAddress: c.localID,
		})

		future.set(err)
	}()

	return future
}

func (c *client) getLeaderClient() (p.DChanServiceClient, error) {
	addr, _ := c.raft.LeaderWithID()
	return c.getClient(addr)
}

func (c *client) getClient(address raft.ServerAddress) (p.DChanServiceClient, error) {
	conn, err := c.cm.Register(address)
	if err != nil {
		return nil, err
	}

	return p.NewDChanServiceClient(conn), nil
}


