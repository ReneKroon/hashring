package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ReneKroon/hashring/internal"
	"github.com/ReneKroon/hashring/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var port = flag.Int("port", 7070, "Set the port to listen to for this server")

func main() {

	flag.Parse()
	var opts []grpc.DialOption

	var server = fmt.Sprintf("%s:%d", internal.GetLocalIP(), *port)
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(server, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := proto.NewHashStoreClient(conn)

	for {
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("test%d", i)
			slowPut(client, key, "data")

			if data, err := slowGet(client, key); err == nil {
				if data.Found {
					fmt.Printf("-> Add %d\t%s\n", i, *data.Data)
				} else {
					fmt.Println("Not found")
				}
			} else {

				fmt.Println("->" + err.Error())
			}

			time.Sleep(time.Millisecond * 150)
		}
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("test%d", i)
			if data, err := slowGet(client, key); err == nil {
				if data.Found {
					fmt.Println(*data.Data)
				} else {
					fmt.Println("Not found")
				}
			} else {

				fmt.Println(err)
			}
			slowRemove(client, key)
		}
	}
}

func slowRemove(client proto.HashStoreClient, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dataToRemove := proto.Key{Key: key}
	if status, err := client.Remove(ctx, &dataToRemove); err != nil {
		log.Println(err.Error())
	} else {
		log.Println(status)
	}
}

func slowPut(client proto.HashStoreClient, key string, data string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	dataToPut := proto.KeyData{Key: key, Data: data}
	if status, err := client.Put(ctx, &dataToPut); err != nil {
		log.Println(err.Error())
	} else {
		log.Println(status)
	}

}

func slowGet(client proto.HashStoreClient, key string) (*proto.Data, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return client.Get(ctx, &proto.Key{Key: key})
}
