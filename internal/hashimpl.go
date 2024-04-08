package internal

import (
	"hash/crc32"
	"net/netip"

	"github.com/ReneKroon/hashring"
)

type HashCrc32 struct {
}

func NewHasher() hashring.Hasher {
	return HashCrc32{}
}

func (h HashCrc32) Hash(key []byte) uint32 {
	crc := crc32.NewIEEE()
	crc.Write(key)

	return crc.Sum32()
}

func (h HashCrc32) HashPeer(peer netip.AddrPort) uint32 {
	return h.Hash([]byte(peer.String()))
}

func (h HashCrc32) HashString(s string) uint32 {
	return h.Hash([]byte(s))
}
