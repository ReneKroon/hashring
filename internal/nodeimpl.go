package internal

import (
	"log"
	"math"
	"net/netip"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type client struct {
	*grpc.ClientConn
	proto.HashStoreClient
}

type NodeImpl struct {
	hashring.Hasher
	peerList map[uint32]*client
	self     uint32
}

func NewNodeImpl(inital []netip.AddrPort, self netip.AddrPort, h hashring.Hasher) hashring.Node {
	p := map[uint32]*client{}
	for _, r := range inital {

		p[h.HashPeer(r)] = createClient(r)
	}
	p[h.HashPeer(self)] = nil
	return &NodeImpl{h, p, h.HashPeer(self)}
}

func (n *NodeImpl) AddNode(peer netip.AddrPort) {
	n.peerList[n.HashPeer(peer)] = createClient(peer)
}

func (n *NodeImpl) RemoveNode(peer netip.AddrPort) {
	if v, ok := n.peerList[n.HashPeer(peer)]; ok {
		v.Close()

		delete(n.peerList, n.HashPeer(peer))
	}

}

func (n *NodeImpl) GetNode(key string) (proto.HashStoreClient, bool) {

	// Find the node that has the checksum just preceding this data checksum
	// Else it's the last node
	crc32 := n.HashString(key)

	var maxKey *proto.HashStoreClient
	var maxCrc uint32

	var beforeKey *proto.HashStoreClient
	var beforeDelta uint32 = math.MaxUint32
	var beforeCrc uint32

	for nodeCrc, client := range n.peerList {

		// find max node on ring
		if nodeCrc > maxCrc {
			if client != nil {
				maxKey = &client.HashStoreClient
			} else {
				maxKey = nil
			}
			maxCrc = nodeCrc
		}
		//
		if nodeCrc <= crc32 && crc32-nodeCrc < beforeDelta {
			beforeDelta = crc32 - nodeCrc
			if client != nil {
				beforeKey = &client.HashStoreClient
			} else {
				beforeKey = nil
			}
			beforeCrc = nodeCrc
		}

	}

	if beforeKey != nil {
		return *beforeKey, false
	} else if beforeCrc == n.self {
		return nil, true
	}

	if maxCrc == n.self {
		return nil, true
	}
	return *maxKey, false
}

func createClient(server netip.AddrPort) *client {
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(server.String(), opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	//defer conn.Close()
	return &client{conn, proto.NewHashStoreClient(conn)}

}
