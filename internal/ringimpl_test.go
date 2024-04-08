package internal_test

import (
	"net/netip"
	"testing"

	"github.com/ReneKroon/hashring/internal"
)

func TestNewRing(t *testing.T) {
	internal.NewRing([]netip.AddrPort{}, netip.MustParseAddrPort("192.168.0.1:7070"))

}
