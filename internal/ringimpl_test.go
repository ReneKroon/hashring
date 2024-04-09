package internal_test

import (
	"net/netip"
	"testing"

	"github.com/ReneKroon/hashring/internal"
)

func TestNewRing(t *testing.T) {
	hasher := internal.NewHasher()
	node := internal.NewNodeImpl([]netip.AddrPort{}, netip.MustParseAddrPort("192.168.0.1:7070"), hasher)

	internal.NewRing(node, hasher)

}
