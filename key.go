package hashring

import "github.com/ReneKroon/hashring/proto"

type Key interface {
	Get(key string) (data string, found bool)
	Put(key, data string)
	PutMany(data []*proto.KeyData)
	Remove(key string)
	Rebalance(node Node) (kept, sent int)
}

type ClientKey interface {
	Key
}

type ServerKey interface {
	Key
}
