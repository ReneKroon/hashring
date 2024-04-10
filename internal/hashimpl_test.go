package internal_test

import (
	"net/netip"
	"testing"

	"github.com/ReneKroon/hashring/internal"
	"github.com/stretchr/testify/assert"
)

func TestMe(t *testing.T) {

	s := "Crcme"
	h := internal.NewHasher()

	h.HashString(s)
	h.HashPeer(netip.MustParseAddrPort("192.168.0.1:7070"))

}

func TestGetNodeForHash(t *testing.T) {

	h := internal.NewHasher()

	self := h.HashPeer(netip.MustParseAddrPort("192.168.0.1:7070"))
	node2 := h.HashPeer(netip.MustParseAddrPort("192.168.0.1:7071"))
	node3 := h.HashPeer(netip.MustParseAddrPort("192.168.0.1:7072"))

	nodes := []uint32{self, node2, node3}

	crc := h.HashString("aKey")

	targetHash, isSelf := h.GetNodeForHash(crc, nodes, self)
	assert.Equal(t, node2, targetHash, "Selected node should be node2")

	targetHash, isSelf = h.GetNodeForHash(h.HashString("aKey4"), nodes, self)
	assert.Equal(t, self, targetHash, "Selected node should be self")
	assert.Equal(t, isSelf, true, "Selected node should be self")

}

func TestGetNodeForHash_order(t *testing.T) {

	h := internal.NewHasher()

	self := h.HashPeer(netip.MustParseAddrPort("192.168.0.1:7070"))
	node3 := h.HashPeer(netip.MustParseAddrPort("192.168.0.1:7072"))

	nodes := []uint32{self, node3}

	crc := h.HashString("test368")

	targetHash, isSelf := h.GetNodeForHash(crc, nodes, self)
	assert.Equal(t, self, targetHash, "Selected node should be self")
	assert.Equal(t, isSelf, true, "Selected node should be self, coming from self")

}

func TestGetNodeForHash_order_reverse(t *testing.T) {

	h := internal.NewHasher()

	self := h.HashPeer(netip.MustParseAddrPort("192.168.0.1:7070"))
	node3 := h.HashPeer(netip.MustParseAddrPort("192.168.0.1:7072"))

	nodes := []uint32{node3, self}

	crc := h.HashString("test368")

	targetHash, isSelf := h.GetNodeForHash(crc, nodes, node3)
	assert.Equal(t, self, targetHash, "Selected node should be self")
	assert.Equal(t, isSelf, false, "Selected node should be self, coming from node3")

}
