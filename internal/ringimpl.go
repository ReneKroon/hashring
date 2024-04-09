package internal

import (
	"context"
	"log"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/proto"
)

type SingleRing struct {
	hashring.Hasher
	hashring.Node
	k hashring.ServerKey
	proto.UnimplementedHashStoreServer
}

func NewRing(node hashring.Node, hasher hashring.Hasher) hashring.Ring {

	keys := NewKeyImpl()

	return SingleRing{

		hasher,
		node,
		keys,
		proto.UnimplementedHashStoreServer{},
	}
}

func (r SingleRing) Get(ctx context.Context, k *proto.Key) (*proto.Data, error) {

	p := &proto.Data{}

	if client, self := r.GetNode(k.Key); self {

		p.Data, p.Found = r.k.Get(k.Key)
		log.Println("Retrieving a key ", k.Key, p.Data, p.Found)
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
