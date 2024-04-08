package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/ReneKroon/hashring/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var port = flag.Int("port", 7070, "Set the port to listen to for this server")

func main() {

	flag.Parse()
	var opts []grpc.DialOption

	var server = fmt.Sprintf("%s:%d", GetLocalIP(), *port)
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(server, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := proto.NewHashStoreClient(conn)

	for i := 0; ; i++ {
		dataToPut := proto.KeyData{Key: fmt.Sprintf("test%d", i), Data: "data"}
		client.Put(context.Background(), &dataToPut)
		data, err := client.Get(context.Background(), &proto.Key{Key: "test"})

		fmt.Println(err)
		fmt.Println(*data.Data)
		i++
	}
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
