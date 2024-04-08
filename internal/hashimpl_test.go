package internal_test

import (
	"testing"

	"github.com/ReneKroon/hashring/internal"
)

func TestMe(t *testing.T) {

	s := "Crcme"
	h := internal.NewHasher()
	h.Hash([]byte(s))
	h.HashString(s)

}
