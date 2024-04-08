package hashring

import "net/netip"

// Holds the algorithm for the network
type Hasher interface {
	Hash(key []byte) []byte
	HashPeer(peer netip.AddrPort) []byte
	HashString(key string) []byte
}
