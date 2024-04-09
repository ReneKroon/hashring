package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

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
		key := fmt.Sprintf("test%d", i)
		dataToPut := proto.KeyData{Key: key, Data: "data"}
		client.Put(context.Background(), &dataToPut)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		if data, err := client.Get(ctx, &proto.Key{Key: key}); err == nil {
			if data.Found {
				fmt.Println(*data.Data)
			} else {
				fmt.Println("Not found")
			}
		} else {

			fmt.Println(err)
		}
		cancel()
		i++
		time.Sleep(time.Millisecond * 50)
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
