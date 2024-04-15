package internal

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/proto"
	"google.golang.org/grpc"
)

type Server struct {
	*grpc.Server
	node hashring.Node
	ring hashring.Ring
	lis  net.Listener
}

func NewServer(ring hashring.Ring, node hashring.Node, lis net.Listener) *Server {
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	proto.RegisterHashStoreServer(grpcServer, ring)
	proto.RegisterNodeStatusServer(grpcServer, node)

	return &Server{grpcServer, node, ring, lis}
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

func (s *Server) Run() error {
	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {

		sig := <-sigs
		fmt.Println("Caught signal")
		fmt.Println()
		fmt.Println(sig)
		s.node.Shutdown()
		s.Server.GracefulStop()

	}()
	return s.Server.Serve(s.lis)

}

func (s *Server) Stop() {
	s.Server.GracefulStop()
}
