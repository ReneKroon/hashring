package internal_test

import (
	"fmt"
	"net/netip"
	"testing"

	"github.com/ReneKroon/hashring/internal"
)

func TestRebalance(t *testing.T) {

	h := internal.NewHasher()

	self := netip.MustParseAddrPort("192.168.0.1:7070")
	node3 := netip.MustParseAddrPort("192.168.0.1:7072")

	nodes := []netip.AddrPort{node3, self}

	k := internal.NewServerKeyImpl()
	node := internal.NewNodeImpl(nodes, self, h, k, nil)
	for i := 0; i < 100; i++ {
		k.Put(fmt.Sprintf("key%d", i*101), "data")
	}

	// rebalance
	kept, sent := k.Rebalance(node)
	if kept < 20 || sent < 20 {
		t.FailNow()
	}
}
