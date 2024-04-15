package hashring

type Key interface {
	Get(key string) (data string, found bool)
	Put(key, data string)
	Remove(key string)
	Rebalance(node Node) (kept, sent int)
}

type ClientKey interface {
	Key
}

type ServerKey interface {
	Key
}
