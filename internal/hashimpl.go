package internal

import (
	"hash/crc32"
	"net/netip"
	"sort"

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

// getNodeForHash returns the owning vnode hash for crc32 on a consistent
// hash ring. It picks the largest entry in peerList that is <= crc32
// (predecessor); if no such entry exists, it wraps to the maximum.
//
// peerList MUST be sorted ascending. Callers maintain the sort once per
// topology change so each lookup is O(log n) via binary search rather than
// O(n). The returned bool reports whether the chosen hash equals self.
func getNodeForHash(crc32 uint32, peerList []uint32, self uint32) (uint32, bool) {
	if len(peerList) == 0 {
		panic("getNodeForHash: empty peer list")
	}

	// First index whose hash is strictly greater than crc32; the predecessor
	// is the entry immediately before it. If every entry is greater (idx==0)
	// or every entry is <= (idx==len), we end up at peerList[len-1] — the
	// ring's maximum, which is the wrap-around owner.
	idx := sort.Search(len(peerList), func(i int) bool {
		return peerList[i] > crc32
	})
	if idx == 0 {
		idx = len(peerList)
	}
	hash := peerList[idx-1]
	return hash, hash == self
}
