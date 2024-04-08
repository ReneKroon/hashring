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

func main() {

	flag.Parse()
	var opts []grpc.DialOption

	var server = fmt.Sprintf("%s:7070", GetLocalIP())
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(server, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := proto.NewHashStoreClient(conn)

	dataToPut := proto.KeyData{Key: "test", Data: "data"}
	client.Put(context.Background(), &dataToPut)
	data, err := client.Get(context.Background(), &proto.Key{Key: "test"})

	fmt.Println(err)
	fmt.Println(*data.Data)
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
