package internal

import (
	"net/netip"

	"github.com/ReneKroon/hashring"
)

type SingleRing struct {
	hashring.Hasher
	hashring.Node
	k hashring.ServerKey
}

func NewRing(seeds []netip.AddrPort) hashring.Ring {

	hasher := NewHasher()
	node := NewNodeImpl(seeds, hasher)
	keys := NewKeyImpl()

	return SingleRing{
		hasher,
		node,
		keys,
	}
}

func (r SingleRing) Get(key string) string {
	return r.k.Get(r.GetNode(key), key)
}
func (r SingleRing) Put(key string, data string) {

	// hash
	// determine node
	// store at node

	node := r.GetNode(key)

	r.k.Put(node, key, data)
}

func (r SingleRing) Remove(key string) {

	r.k.Remove(r.GetNode(key), key)
}
