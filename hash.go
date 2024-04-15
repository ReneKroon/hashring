package hashring

import "net/netip"

// Holds the algorithm for the network
type Hasher interface {
	HashPeer(peer netip.AddrPort) (hash uint32)
	HashString(key string) uint32
	GetNodeForHash(crc32 uint32, peerList []uint32, self uint32) (uint32, bool)
}
