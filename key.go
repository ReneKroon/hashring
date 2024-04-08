package hashring

type Key interface {
	Get(key string) (*string, bool)
	Put(key, data string)
	Remove(key string)
}

type ClientKey interface {
	Key
}

type ServerKey interface {
	Key
}
