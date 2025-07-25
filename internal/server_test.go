package internal_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/ReneKroon/hashring/internal"
	"github.com/ReneKroon/hashring/proto"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

var buffers = map[netip.AddrPort]*bufconn.Listener{}
var keyStores = map[*internal.Server]*internal.ServerKeyImpl{}

//var masterNode = netip.AddrPortFrom(netip.MustParseAddr("192.168.0.1"), nextPort)

var nextPort uint16 = 7070

func newHost() netip.AddrPort {
	next := netip.AddrPortFrom(netip.MustParseAddr("192.168.0.1"), nextPort)
	// better CRC spread, the crc32 diff between 7070 and 7071 is unfavourable
	nextPort += 10
	return next
}

func CreateTestClient(server netip.AddrPort) *internal.Client {

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

	buffer := 1024 * 1024
	lis := bufconn.Listen(buffer)

	buffers[addrPort] = lis
	return lis
}

func cleanNodeBuffers() {
	for _, v := range buffers {
		v.Close()
	}
}

func AddServer(t *testing.T, home netip.AddrPort, masterNode netip.AddrPort) *internal.Server {

	lis := addNodeBuffer(home)

	keys := internal.NewServerKeyImpl()
	hasher := internal.NewHasher()

	node := internal.NewNodeImpl([]netip.AddrPort{masterNode}, home, hasher, keys, CreateTestClient)

	ring := internal.NewRing(node, hasher, keys)

	server := internal.NewServer(ring, node, lis)

	keyStores[server] = keys.(*internal.ServerKeyImpl)

	go func() { server.Run() }()

	return server
}

func TestIntegration(t *testing.T) {

	home := newHost()

	server := AddServer(t, home, home)
	defer server.Stop()
	defer cleanNodeBuffers()

	conn := CreateTestClient(home)

	client := proto.NewHashStoreClient(conn)
	nodeClient := proto.NewNodeStatusClient(conn)

	list, _ := nodeClient.GetNodeList(context.Background(), &proto.NodeList{})

	assert.Equal(t, list.Node[0].Port, uint32(7070), "We registered on port 7070")

	client.Put(context.Background(), &proto.KeyData{Key: "key", Data: "data"})
	data, _ := client.Get(context.Background(), &proto.Key{Key: "key"})

	assert.Equal(t, true, data.Found, "Message should have been found")
	assert.Equal(t, "data", *data.Data, "Message Data should have been found")

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, err := client.Put(context.Background(), &proto.KeyData{Key: fmt.Sprintf("key%d", i), Data: fmt.Sprintf("data%d", i)})
			if err != nil || ok.Ok == false {
				t.Logf("Cannot set test data: %s", err)

			}
		}(i)

	}
	wg.Wait()

	server2 := AddServer(t, newHost(), home)

	<-time.After(time.Second)
	// check both stores have keys (rebalance)
	assert.True(t, len(keyStores[server].LocalData) > 30, fmt.Sprintf("Length was only %d", mapCount(keyStores[server].LocalData)))
	assert.True(t, len(keyStores[server2].LocalData) > 30, fmt.Sprintf("Length was only %d", mapCount(keyStores[server2].LocalData)))
	// cleanup

	// the stop will check that no locks occur during rebalancing etc.
	server2.Stop()
	<-time.After(time.Second)
	// check succesful transfer back to remaining server
	assert.True(t, len(keyStores[server].LocalData) == 101, fmt.Sprintf("Length was only %d", mapCount(keyStores[server].LocalData)))
}

func TestRebalanceIntegration(t *testing.T) {
	defer cleanNodeBuffers()

	home := newHost()
	server := AddServer(t, home, home)
	defer server.Stop()

	server2 := AddServer(t, newHost(), home)
	defer server2.Stop()

	server3 := AddServer(t, newHost(), home)

	conn := CreateTestClient(home)
	client := proto.NewHashStoreClient(conn)

	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, err := client.Put(context.Background(), &proto.KeyData{Key: fmt.Sprintf("key%d", i), Data: fmt.Sprintf("data%d", i)})
			if err != nil || ok.Ok == false {
				t.Logf("Cannot set test data: %s", err)
				t.FailNow()
			}
		}(i)

	}
	wg.Wait()

	server2.Stop()

	<-time.After(time.Second)

	allKeys := len(keyStores[server].LocalData) + len(keyStores[server3].LocalData)
	assert.True(t, allKeys == 100, fmt.Sprintf("Check that the keys are rebalanced to the other servers, 100 total but found %d", allKeys))
	assert.True(t, len(keyStores[server2].LocalData) == 0, fmt.Sprintf("Check that store 2 was emptied, should have been zero but got %d", len(keyStores[server2].LocalData)))

}

func mapCount[K comparable, V any](m map[K]V) int {
	count := 0
	for range m {
		count++
	}
	return count
}
