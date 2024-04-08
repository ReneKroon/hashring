package internal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net/netip"

	"github.com/ReneKroon/hashring"
)

type HashCrc32 struct {
}

func NewHasher() hashring.Hasher {
	return HashCrc32{}
}

func (h HashCrc32) Hash(key []byte) []byte {
	crc := crc32.NewIEEE()
	crc.Write(key)
	a := make([]byte, 4)
	binary.LittleEndian.PutUint32(a, crc.Sum32())
	return a
}

func (h HashCrc32) HashPeer(peer netip.AddrPort) []byte {
	return h.Hash([]byte(fmt.Sprintf("%s", peer)))
}

func (h HashCrc32) HashString(s string) []byte {
	return h.Hash([]byte(s))
}
