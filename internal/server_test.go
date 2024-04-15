package internal_test

import (
	"context"
	"log"
	"net"
	"net/netip"
	"testing"

	"github.com/ReneKroon/hashring/internal"
	"github.com/ReneKroon/hashring/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

var buffers map[netip.AddrPort]*bufconn.Listener

func CreateTestClient(server netip.AddrPort) *internal.Client {
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.DialContext(context.Background(), "",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return buffers[server].Dial()
		}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("error connecting to server: %v", err)
	}

	//defer conn.Close()
	return &internal.Client{conn, proto.NewHashStoreClient(conn), proto.NewNodeStatusClient(conn), server}

}

func addNodeBuffer(addrPort netip.AddrPort) *bufconn.Listener {
	if buffers == nil {
		buffers = map[netip.AddrPort]*bufconn.Listener{}
	}

	buffer := 1024 * 1024
	lis := bufconn.Listen(buffer)

	buffers[addrPort] = lis
	return lis
}

func TestIntegration(t *testing.T) {

	address := internal.GetLocalIP()

	home := netip.AddrPortFrom(netip.AddrFrom4([4]byte(address.To4())), 7070)

	lis := addNodeBuffer(home)

	keys := internal.NewKeyImpl()
	hasher := internal.NewHasher()

	node := internal.NewNodeImpl([]netip.AddrPort{home}, home, hasher, keys, CreateTestClient)

	ring := internal.NewRing(node, hasher, keys)

	server := internal.NewServer(ring, node, lis)

	go func() { server.Run() }()

	conn := CreateTestClient(home)

	client := proto.NewHashStoreClient(conn)
	nodeClient := proto.NewNodeStatusClient(conn)

	/*
		closer := func() {
			err := lis.Close()
			if err != nil {
				log.Printf("error closing listener: %v", err)
			}
			server.Stop()
		}*/

	list, _ := nodeClient.GetNodeList(context.Background(), &emptypb.Empty{})

	assert.Equal(t, list.Node[0].Port, uint32(7070), "We registered on port 7070")

	client.Put(context.Background(), &proto.KeyData{Key: "key", Data: "data"})
	data, _ := client.Get(context.Background(), &proto.Key{Key: "key"})

	assert.Equal(t, true, data.Found, "Message should have been found")
	assert.Equal(t, "data", *data.Data, "Message Data should have been found")

	//return client, closer
	server.Stop()
	lis.Close()
}
