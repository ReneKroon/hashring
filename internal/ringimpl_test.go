package internal_test

import (
	"net/netip"
	"testing"

	"github.com/ReneKroon/hashring/internal"
)

func TestNewRing(t *testing.T) {
	internal.NewRing([]netip.AddrPort{})

}
