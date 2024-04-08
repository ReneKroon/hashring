package main

import (
	"fmt"
	"log"
	"net"
	"net/netip"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/internal"
	"github.com/ReneKroon/hashring/proto"
	"google.golang.org/grpc"
)

var ringInfo hashring.Ring

// https://grpc.io/docs/languages/go/basics/
func main() {

	port := 7070
	address := GetLocalIP()
	h := fmt.Sprintf("%s:%d", address, port)
	ip, err := netip.ParseAddrPort(h)
	if err != nil {
		panic(err)
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(h))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Println("listening on :", ip)
	var opts []grpc.ServerOption

	ring := internal.NewRing([]netip.AddrPort{ip})

	grpcServer := grpc.NewServer(opts...)
	proto.RegisterHashStoreServer(grpcServer, ring)

	grpcServer.Serve(lis)

}

// GetLocalIP returns the non loopback local IP of the host
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && !ipnet.IP.IsUnspecified() && !ipnet.IP.IsLinkLocalUnicast() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}
