package main

import (
	"fmt"
	"net"
	"net/netip"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/internal"
)

var ringInfo hashring.Ring

func main() {

	address := GetLocalIP()
	h := fmt.Sprintf("%s:8080", address)
	ip, err := netip.ParseAddrPort(h)
	if err != nil {
		panic(err)
	}

	ring := internal.NewRing([]netip.AddrPort{ip})
	ring.Put("test", "data")
	ring.Get("test")
}

// GetLocalIP returns the non loopback local IP of the host
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
