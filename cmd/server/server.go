package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/netip"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/internal"
)

var ringInfo hashring.Ring

var port = flag.Int("port", 7070, "Set the port to listen to for this server")

// https://grpc.io/docs/languages/go/basics/
func main() {
	flag.Parse()

	address := internal.GetLocalIP()

	ip := netip.AddrPortFrom(netip.AddrFrom4([4]byte(address.To4())), uint16(*port))

	lis, err := net.Listen("tcp", fmt.Sprintf("%s", ip.String()))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Println("listening on :", ip)

	home := netip.AddrPortFrom(netip.AddrFrom4([4]byte(address.To4())), 7070)

	keys := internal.NewServerKeyImpl()
	hasher := internal.NewHasher()
	node := internal.NewNodeImpl([]netip.AddrPort{home}, ip, hasher, keys, internal.CreateClient)

	ring := internal.NewRing(node, hasher, keys)

	server := internal.NewServer(ring, node, lis)

	server.Run()

}
