package internal

import (
	"context"
	"fmt"
	"log"
	"net/netip"
	"sync"
	"time"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type client struct {
	*grpc.ClientConn
	proto.HashStoreClient
	proto.NodeStatusClient
	netip.AddrPort
}

type NodeImpl struct {
	hashring.Hasher
	peerList map[uint32]*client
	selfHash uint32
	self     netip.AddrPort
	proto.UnimplementedNodeStatusServer
}

func NewNodeImpl(inital []netip.AddrPort, self netip.AddrPort, h hashring.Hasher) hashring.Node {
	p := map[uint32]*client{}
	gotList := false
	var list *proto.NodeList
	for _, r := range inital {
		if h.HashPeer(r) == h.HashPeer(self) {
			continue
		}
		client := createClient(r)
		p[h.HashPeer(r)] = client
		if !gotList {
			var err error
			if list, err = client.NodeStatusClient.GetNodeList(context.Background(), &emptypb.Empty{}); err == nil {
				gotList = true
				// process list
			} else {
				panic(err)
			}
		}
	}
	p[h.HashPeer(self)] = nil

	nImpl := &NodeImpl{h, p, h.HashPeer(self), self, proto.UnimplementedNodeStatusServer{}}
	if gotList {
		for _, node := range list.Node {
			nImpl.AddNode(context.Background(), node)
		}
	}
	return nImpl
}

func (n *NodeImpl) AddNode(ctx context.Context, node *proto.Node) (*emptypb.Empty, error) {
	log.Println("Add a node ", node.Host, node.Port)
	if peer, err := netip.ParseAddrPort(fmt.Sprintf("%s:%d", node.Host, node.Port)); err == nil {
		client := createClient(peer)
		n.peerList[n.HashPeer(peer)] = client
		client.AddNode(ctx, &proto.Node{Host: n.self.Addr().String(), Port: uint32(n.self.Port())})
	}

	return &emptypb.Empty{}, nil
}
func (n *NodeImpl) GetNodeList(context.Context, *emptypb.Empty) (*proto.NodeList, error) {
	list := &proto.NodeList{}
	for _, v := range n.peerList {
		if v == nil {
			continue
		}
		list.Node = append(list.Node, &proto.Node{Host: v.Addr().String(), Port: uint32(v.Port())})
	}
	list.Node = append(list.Node, &proto.Node{Host: n.self.Addr().String(), Port: uint32(n.self.Port())})
	return list, nil
}

func (n *NodeImpl) RemoveNode(ctx context.Context, node *proto.Node) (*emptypb.Empty, error) {

	if peer, err := netip.ParseAddrPort(fmt.Sprintf("%s:%d", node.Host, node.Port)); err == nil {
		log.Println("Removing a node ", peer.String())
		if v, ok := n.peerList[n.HashPeer(peer)]; ok {
			delete(n.peerList, n.HashPeer(peer))
			go func() {
				v.Close()
			}()

		}
	}
	return &emptypb.Empty{}, nil
}

func (n *NodeImpl) GetSelf() (peer netip.AddrPort) {
	return n.self
}

// Returns the remote HashStoreClient, nil, true if this node is the right node for the data.
func (n *NodeImpl) GetNode(key string) (proto.HashStoreClient, bool) {

	// Find the node that has the checksum just preceding this data checksum
	// Else it's the last node
	crc32 := n.HashString(key)

	peerList := []uint32{}
	for k := range n.peerList {
		peerList = append(peerList, k)
	}

	hash, self := n.GetNodeForHash(crc32, peerList, n.selfHash)

	if self {
		return nil, true
	} else {
		return n.peerList[hash], false
	}
}

func (n *NodeImpl) Shutdown() {

	self := &proto.Node{Host: n.self.Addr().String(), Port: uint32(n.self.Port())}

	var wg sync.WaitGroup
	for k, p := range n.peerList {
		if k != n.selfHash {
			wg.Add(1)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*40)
				p.NodeStatusClient.RemoveNode(ctx, self)
				defer cancel()
				defer wg.Done()
			}()
		}
	}
	wg.Wait()
}

func createClient(server netip.AddrPort) *client {
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(server.String(), opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	//defer conn.Close()
	return &client{conn, proto.NewHashStoreClient(conn), proto.NewNodeStatusClient(conn), server}

}
