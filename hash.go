package hashring

import "net/netip"

// Holds the algorithm for the network
type Hasher interface {
	HashPeer(peer netip.AddrPort) uint32
	HashString(key string) uint32
}
