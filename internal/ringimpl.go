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

func NewRing(node hashring.Node, hasher hashring.Hasher, keys hashring.ServerKey) hashring.Ring {

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
		var d string
		d, p.Found = r.k.Get(k.Key)
		if p.Found {
			p.Data = &d
		}
		log.Println("Retrieving a key ", k.Key, p.Data, p.Found)
		return p, nil
	} else {
		if p, err := client.Get(ctx, k); err == nil {
			log.Println("Retrieving a key ", k.Key, p.Data, p.Found)
			return p, err
		} else {
			log.Println(err.Error())
			return p, err
		}

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
		log.Println("forward ", k.Key)
		if client == nil || k == nil {
			panic("client and data should always exist for not self node")
		}
		return client.Put(ctx, k)
	}

}

func (r SingleRing) PutMany(ctx context.Context, k *proto.KeyDataList) (*proto.UpdateStatus, error) {
	// hash
	// determine node
	// store at node
	p := &proto.UpdateStatus{Ok: true}
	var err error = nil

	keep := []*proto.KeyData{}

	for _, k := range k.KeyData {

		if client, self := r.GetNode(k.Key); self {
			log.Println("Storing a key")
			keep = append(keep, k)
		} else {
			log.Println("forward ", k.Key)
			if client == nil || k == nil {
				panic("client and data should always exist for not self node")
			}
			if status, err := client.Put(ctx, k); err != nil || !status.Ok {
				log.Println("Error forwarding key, storing locally")
				r.k.Put(k.Key, k.Data)

			}
		}
	}
	r.k.PutMany(keep)
	return p, err
}

func (r SingleRing) Remove(ctx context.Context, k *proto.Key) (*proto.UpdateStatus, error) {
	p := &proto.UpdateStatus{}

	if client, self := r.GetNode(k.Key); self {
		r.k.Remove(k.Key)
		p.Ok = true
		return p, nil
	} else {
		return client.Remove(ctx, k)
	}
}
