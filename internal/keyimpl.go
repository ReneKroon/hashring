package internal

import (
	"net/netip"

	"github.com/ReneKroon/hashring"
)

type ServerKeyImpl struct {
	//store map[string]string
	nodeStore map[netip.AddrPort]map[string]string
}

func NewKeyImpl() hashring.ServerKey {
	return ServerKeyImpl{make(map[netip.AddrPort]map[string]string)}
}

func (k ServerKeyImpl) Get(node netip.AddrPort, key string) string {
	if v, found := k.nodeStore[node]; found {
		if data, dataFound := v[key]; dataFound {
			return data
		}
	}
	return ""
}
func (k ServerKeyImpl) Put(node netip.AddrPort, key string, data string) {

	if _, found := k.nodeStore[node]; !found {
		k.nodeStore[node] = map[string]string{}
	}
	targetStore := k.nodeStore[node]
	targetStore[key] = data

}
func (k ServerKeyImpl) Remove(node netip.AddrPort, key string) {
	if targetStore, found := k.nodeStore[node]; found {
		delete(targetStore, key)
	}
}
