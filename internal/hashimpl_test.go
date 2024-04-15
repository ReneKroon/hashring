package internal_test

import (
	"fmt"
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
	assert.Equal(t, isSelf, false, "Selected node should be node2")

	targetHash, isSelf = h.GetNodeForHash(h.HashString("aKey4"), nodes, self)
	assert.Equal(t, self, targetHash, "Selected node should be self")
	assert.Equal(t, isSelf, true, "Selected node should be self")

	results := map[uint32]int{}
	for i := 0; i < 100; i++ {
		host, _ := h.GetNodeForHash(h.HashString(fmt.Sprintf("key%d", i*101)), nodes, self)
		results[host]++
	}
	for _, v := range results {
		assert.True(t, v > 20)
	}
}

// Testdata shows taht indeed the nodes hash unfavourably to 3050004111 && 3268177433, meaning that these 100 test keys map to 1 node.
func TestGetNodeForHash_rebalance(t *testing.T) {
	h := internal.NewHasher()

	self := h.HashPeer(netip.MustParseAddrPort("192.168.0.1:7070"))
	node3 := h.HashPeer(netip.MustParseAddrPort("192.168.0.1:7071"))

	nodes := []uint32{self, node3}

	t.Logf("Checksum self: %d\n", self)
	t.Logf("Checksum node3: %d\n", node3)

	selfcount := 0
	for i := 0; i < 100; i++ {
		input := fmt.Sprintf("key%d", 101*i)
		_, me := h.GetNodeForHash(h.HashString(input), nodes, self)
		//t.Logf("Checksum: %d\n", h.HashString(input))
		if h.HashString(input) > 3050004111 && h.HashString(input) < 3268177433 {
			panic("SDAFADS")
		}
		if me {
			selfcount++
		}
	}

	assert.True(t, selfcount == 0)
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
