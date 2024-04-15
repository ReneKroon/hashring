package internal_test

import (
	"net/netip"
	"testing"

	"github.com/ReneKroon/hashring/internal"
)

func TestNewRing(t *testing.T) {
	keys := internal.NewKeyImpl()
	hasher := internal.NewHasher()
	node := internal.NewNodeImpl([]netip.AddrPort{}, netip.MustParseAddrPort("192.168.0.1:7070"), hasher, keys, internal.CreateClient)

	internal.NewRing(node, hasher, keys)

}
