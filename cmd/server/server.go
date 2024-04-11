package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/netip"
	"os"
	"os/signal"
	"syscall"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/internal"
	"github.com/ReneKroon/hashring/proto"
	"google.golang.org/grpc"
)

var ringInfo hashring.Ring

var port = flag.Int("port", 7070, "Set the port to listen to for this server")

// https://grpc.io/docs/languages/go/basics/
func main() {
	flag.Parse()

	address := GetLocalIP()

	ip := netip.AddrPortFrom(netip.AddrFrom4([4]byte(address.To4())), uint16(*port))

	lis, err := net.Listen("tcp", fmt.Sprintf(ip.String()))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	log.Println("listening on :", ip)
	var opts []grpc.ServerOption

	home := netip.AddrPortFrom(netip.AddrFrom4([4]byte(address.To4())), 7070)

	keys := internal.NewKeyImpl()
	hasher := internal.NewHasher()
	node := internal.NewNodeImpl([]netip.AddrPort{home}, ip, hasher, keys)

	ring := internal.NewRing(node, hasher, keys)

	grpcServer := grpc.NewServer(opts...)
	proto.RegisterHashStoreServer(grpcServer, ring)
	proto.RegisterNodeStatusServer(grpcServer, node)

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {

		sig := <-sigs
		fmt.Println("Caught signal")
		fmt.Println()
		fmt.Println(sig)
		node.Shutdown()
		grpcServer.Stop()

	}()
	grpcServer.Serve(lis)

}

// GetLocalIP returns the non loopback local IP of the host
func GetLocalIP() net.IP {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		panic("No available local ips")
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback the display it
		if ipnet, ok := address.(*net.IPNet); ok && !(ipnet.IP.IsLoopback() || ipnet.IP.IsUnspecified() || ipnet.IP.IsLinkLocalUnicast()) {
			if ipnet.IP.To4() != nil {
				return ipnet.IP
			}
		}
	}
	panic("No available local ips")
}
