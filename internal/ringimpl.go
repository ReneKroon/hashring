package internal

import (
	"context"
	"log"
	"net/netip"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/proto"
)

type SingleRing struct {
	hashring.Hasher
	hashring.Node
	k hashring.ServerKey
	proto.UnimplementedHashStoreServer
	selfHash uint32
}

func NewRing(seeds []netip.AddrPort, self netip.AddrPort) hashring.Ring {

	hasher := NewHasher()
	node := NewNodeImpl(seeds, self, hasher)
	keys := NewKeyImpl()

	return SingleRing{

		hasher,
		node,
		keys,
		proto.UnimplementedHashStoreServer{},
		hasher.HashPeer(self),
	}
}

func (r SingleRing) Get(ctx context.Context, k *proto.Key) (*proto.Data, error) {

	p := &proto.Data{}

	if client, self := r.GetNode(k.Key); self {
		p.Data, p.Found = r.k.Get(k.Key)
		return p, nil
	} else {
		return client.Get(context.Background(), k)
	}

}

func (r SingleRing) Put(ctx context.Context, k *proto.KeyData) (*proto.UpdateStatus, error) {
	// hash
	// determine node
	// store at node
	p := &proto.UpdateStatus{}

	if client, self := r.GetNode(k.Key); self {
		log.Println("Storing a key")
		r.k.Put(k.Key, k.Data)
		p.Ok = true
		return p, nil
	} else {
		return client.Put(context.Background(), k)
	}

}
func (r SingleRing) Remove(ctx context.Context, k *proto.Key) (*proto.UpdateStatus, error) {
	p := &proto.UpdateStatus{}

	if client, self := r.GetNode(k.Key); self {
		r.k.Remove(k.Key)
		p.Ok = true
		return p, nil
	} else {
		return client.Remove(context.Background(), k)
	}
}
