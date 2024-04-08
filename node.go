package hashring

import (
	"net/netip"
)

// Can tell which nodes are in the network
type Node interface {
	AddNode(peer netip.AddrPort)
	RemoveNode(peer netip.AddrPort)
	//GetNodeList() map[netip.AddrPort][]byte
	GetNode(key string) netip.AddrPort
}
