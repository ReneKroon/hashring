package internal_test

import (
	"net/netip"
	"testing"

	"github.com/ReneKroon/hashring/internal"
)

func TestMe(t *testing.T) {

	s := "Crcme"
	h := internal.NewHasher()

	h.HashString(s)
	h.HashPeer(netip.MustParseAddrPort("192.168.0.1:7070"))

}
