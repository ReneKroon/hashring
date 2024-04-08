package internal

import (
	"encoding/binary"
	"math"
	"net/netip"

	"github.com/ReneKroon/hashring"
)

type NodeImpl struct {
	hashring.Hasher
	peerList map[netip.AddrPort][]byte
}

func NewNodeImpl(inital []netip.AddrPort, h hashring.Hasher) hashring.Node {
	p := map[netip.AddrPort][]byte{}
	for _, r := range inital {
		p[r] = h.HashPeer(r)
	}
	return &NodeImpl{h, p}
}

func (n *NodeImpl) AddNode(peer netip.AddrPort) {
	n.peerList[peer] = n.HashPeer(peer)
}
func (n *NodeImpl) RemoveNode(peer netip.AddrPort) {
	delete(n.peerList, peer)
}

func (n *NodeImpl) GetNode(key string) netip.AddrPort {

	// Find the node that has the checksum just preceding this data checksum
	// Else it's the last node
	bytes := n.HashString(key)
	crc32 := binary.LittleEndian.Uint32(bytes)

	var maxKey netip.AddrPort
	var maxCrc uint32

	var beforeKey *netip.AddrPort
	var beforeDelta uint32 = math.MaxUint32
	for k, v := range n.peerList {
		nodeCrc := binary.LittleEndian.Uint32(v)
		// find max node on ring
		if nodeCrc > maxCrc {
			maxKey = k
			maxCrc = nodeCrc
		}
		//
		if nodeCrc <= crc32 && crc32-nodeCrc < beforeDelta {
			beforeDelta = crc32 - nodeCrc
			beforeKey = &k
		}

	}

	if beforeKey != nil {
		return *beforeKey
	}
	return maxKey
}
