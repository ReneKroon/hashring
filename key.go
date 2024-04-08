package hashring

import "net/netip"

type ClientKey interface {
	Get(key string) string
	Put(key, data string)
	Remove(key string)
}

type ServerKey interface {
	Get(node netip.AddrPort, key string) string
	Put(node netip.AddrPort, key, data string)
	Remove(node netip.AddrPort, key string)
}
