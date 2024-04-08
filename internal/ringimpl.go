package internal

import (
	"context"
	"net/netip"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/proto"
)

type SingleRing struct {
	hashring.Hasher
	hashring.Node
	k hashring.ServerKey
	proto.UnimplementedHashStoreServer
}

func NewRing(seeds []netip.AddrPort) hashring.Ring {

	hasher := NewHasher()
	node := NewNodeImpl(seeds, hasher)
	keys := NewKeyImpl()

	return SingleRing{

		hasher,
		node,
		keys,
		proto.UnimplementedHashStoreServer{},
	}
}

func (r SingleRing) Get(ctx context.Context, k *proto.Key) (*proto.Data, error) {
	data := r.k.Get(r.GetNode(k.GetKey()), k.GetKey())

	return &proto.Data{Found: true, Data: &data}, nil

}
func (r SingleRing) Put(ctx context.Context, k *proto.KeyData) (*proto.UpdateStatus, error) {
	// hash
	// determine node
	// store at node

	node := r.GetNode(k.GetKey())

	r.k.Put(node, k.GetKey(), k.GetData())
	return &proto.UpdateStatus{Ok: true, Err: nil}, nil

}
func (r SingleRing) Remove(ctx context.Context, k *proto.Key) (*proto.UpdateStatus, error) {
	r.k.Remove(r.GetNode(k.GetKey()), k.GetKey())
	return &proto.UpdateStatus{Ok: true, Err: nil}, nil
}
