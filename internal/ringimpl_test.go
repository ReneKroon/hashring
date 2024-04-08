package internal_test

import (
	"net/netip"
	"testing"

	"github.com/ReneKroon/hashring/internal"
	"github.com/stretchr/testify/assert"
)

func TestNewRing(t *testing.T) {
	r := internal.NewRing([]netip.AddrPort{})

	r.Put("test", "data")

	assert.Equal(t, "data", r.Get("test"))

	assert.Equal(t, "", r.Get("fail"))
	r.Remove("fail")
}
