package internal

import (
	"hash/crc32"
	"math"
	"net/netip"

	"github.com/ReneKroon/hashring"
)

type HashCrc32 struct {
}

type Farmhash struct {
}

func NewHasher() hashring.Hasher {
	return Farmhash{}

}

func (h HashCrc32) Hash(key []byte) uint32 {
	// Consider Farmhash as a better alternative
	crc := crc32.NewIEEE()
	crc.Write(key)

	return crc.Sum32()
}

func (h HashCrc32) HashPeer(peer netip.AddrPort) (hash uint32) {
	return h.Hash([]byte(peer.String()))

}

func (h HashCrc32) HashString(s string) uint32 {
	return h.Hash([]byte(s))
}

func (h Farmhash) Hash(key []byte) uint32 {
	// Consider Farmhash as a better alternative
	crc := crc32.NewIEEE()
	crc.Write(key)

	return crc.Sum32()
}

func (h Farmhash) HashPeer(peer netip.AddrPort) (hash uint32) {
	return h.Hash([]byte(peer.String()))

}

func (h Farmhash) HashString(s string) uint32 {
	return h.Hash([]byte(s))
}

func (h Farmhash) GetNodeForHash(crc32 uint32, peerList []uint32, self uint32) (uint32, bool) {
	return getNodeForHash(crc32, peerList, self)
}
func (h HashCrc32) GetNodeForHash(crc32 uint32, peerList []uint32, self uint32) (uint32, bool) {
	return getNodeForHash(crc32, peerList, self)
}

func getNodeForHash(crc32 uint32, peerList []uint32, self uint32) (uint32, bool) {
	// Find the node that has the checksum just preceding this data checksum
	// Else it's the last node
	var maxCrc uint32

	var beforeDelta uint32 = math.MaxUint32
	var beforeCrc uint32

	var beforeCrcSet = false
	var maxCrcSet = false

	for _, nodeCrc := range peerList {

		// find max node on ring
		if nodeCrc > maxCrc {
			maxCrc = nodeCrc
			maxCrcSet = true
		}
		//
		if nodeCrc <= crc32 && crc32-nodeCrc < beforeDelta {
			beforeDelta = crc32 - nodeCrc
			beforeCrc = nodeCrc
			beforeCrcSet = true
		}

	}

	if beforeCrcSet {
		return beforeCrc, beforeCrc == self
	}

	if maxCrcSet {
		return maxCrc, maxCrc == self
	}

	panic("Shouldn not get here")
}
