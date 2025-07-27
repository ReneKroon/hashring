package internal

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	*grpc.ClientConn
	proto.HashStoreClient
	proto.NodeStatusClient
	netip.AddrPort
}

type NodeImpl struct {
	hashring.Hasher
	// peerList should include 'self' with a nil value
	peerList map[uint32]*Client
	// list of nodes that are not reachable right now
	offlineList map[uint32]*Client
	selfHash    uint32
	self        netip.AddrPort
	proto.UnimplementedNodeStatusServer
	rebalancer   func(hashring.Node) (int, int)
	createclient func(server netip.AddrPort) (*Client, error)
	nodeLock     sync.RWMutex
	nodeUpdate   chan (hashring.NodeUpdate)
	done         chan struct{}
	isShutdown   atomic.Bool
}

func NewNodeImpl(inital []netip.AddrPort, self netip.AddrPort, h hashring.Hasher, k hashring.ServerKey, createclient func(server netip.AddrPort) (*Client, error)) hashring.Node {
	p := map[uint32]*Client{}

	nImpl := &NodeImpl{
		Hasher:                        h,
		peerList:                      p,
		selfHash:                      h.HashPeer(self),
		self:                          self,
		UnimplementedNodeStatusServer: proto.UnimplementedNodeStatusServer{},
		rebalancer:                    k.Rebalance,
		createclient:                  createclient,
		nodeLock:                      sync.RWMutex{},
		nodeUpdate:                    make(chan hashring.NodeUpdate, 100),
		done:                          make(chan struct{}),
		isShutdown:                    atomic.Bool{},
	}

	for _, v := range inital {
		if v == self {
			continue
		}
		if createclient != nil {

			nImpl.nodeUpdate <- hashring.NodeUpdate{
				Node: &proto.Node{
					Host: v.Addr().String(),
					Port: uint32(v.Port()),
				},
				Status: hashring.Online,
			}
		} else {
			nImpl.peerList[h.HashPeer(v)] = nil
		}
	}
	p[h.HashPeer(self)] = nil

	go nImpl.processUpdates(nImpl.done)
	go nImpl.findNeighbours(nImpl.done)

	return nImpl
}

func (n *NodeImpl) processUpdates(done chan struct{}) {

	for {

		//var node *proto.Node
		var newNodes []hashring.NodeUpdate

		select {
		case <-done:
			log.Println("Shutdown request, exiting processUpdates")
			return
		case node := <-n.nodeUpdate:
			newNodes = append(newNodes, node)
		}

		outstanding := len(n.nodeUpdate)
		for range outstanding {
			newNodes = append(newNodes, <-n.nodeUpdate)
		}

		n.nodeLock.Lock()

		for _, node := range newNodes {

			if peer, err := netip.ParseAddrPort(fmt.Sprintf("%s:%d", node.Node.Host, node.Node.Port)); err == nil && peer != n.self {
				if node.Status == hashring.Online {
					n.addNode(peer, node)
				} else if node.Status == hashring.Offline {
					n.removeNode(peer, node)
				}
			}
		}
		n.nodeLock.Unlock()
		n.tryRebalance()
	}

}

func (n *NodeImpl) addNode(peer netip.AddrPort, node hashring.NodeUpdate) {
	_, found := n.peerList[n.HashPeer(peer)]
	if !found {
		log.Println("Add a node ", node.Node.Host, node.Node.Port)

		if client, err := n.createclient(peer); err == nil {
			n.peerList[n.HashPeer(peer)] = client
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			client.AddNode(ctx, &proto.Node{Host: n.self.Addr().String(), Port: uint32(n.self.Port())})
			cancel()
		} else {
			log.Println("Error creating client for node", peer, ":", err)
		}
	}
}

func (n *NodeImpl) removeNode(peer netip.AddrPort, node hashring.NodeUpdate) {
	client, found := n.peerList[n.HashPeer(peer)]
	if found {
		delete(n.peerList, n.HashPeer(peer))
		n.offlineList[n.HashPeer(peer)] = client
	}
}

func (n *NodeImpl) AddNode(ctx context.Context, node *proto.Node) (*emptypb.Empty, error) {

	n.nodeUpdate <- hashring.NodeUpdate{node, hashring.Online}

	return &emptypb.Empty{}, nil
}

func (n *NodeImpl) GetNodeList(ctx context.Context, newNodes *proto.NodeList) (*proto.NodeList, error) {
	n.nodeLock.RLock()
	defer n.nodeLock.RUnlock()

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
	n.nodeLock.Lock()

	if peer, err := netip.ParseAddrPort(fmt.Sprintf("%s:%d", node.Host, node.Port)); err == nil {
		log.Println("Removing a node ", peer.String())
		if v, ok := n.peerList[n.HashPeer(peer)]; ok {
			delete(n.peerList, n.HashPeer(peer))

			go func() {
				v.Close()
			}()

		}
	}
	n.nodeLock.Unlock()
	n.tryRebalance()
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
	n.nodeLock.RLock()
	defer n.nodeLock.RUnlock()
	for k := range n.peerList {
		peerList = append(peerList, k)
	}

	// Cover edge case on shutdown where no nodes are left.
	if len(peerList) == 0 {
		return nil, true
	}
	hash, self := n.GetNodeForHash(crc32, peerList, n.selfHash)

	if self {
		return nil, true
	} else {
		return n.peerList[hash], false
	}
}

func (n *NodeImpl) findNeighbours(done chan struct{}) {
	for {
		delay := 10 + rand.IntN(10)
		timer := time.NewTimer(time.Second * time.Duration(delay))
		select {
		case <-done:
			timer.Stop()
			return
		case <-timer.C:
			nodes := []*proto.Node{}
			for k, p := range n.peerList {
				if k != n.selfHash && p != nil {
					ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
					myNode := &proto.Node{Host: n.self.Addr().String(), Port: uint32(n.self.Port())}
					if list, err := p.GetNodeList(ctx, &proto.NodeList{Node: []*proto.Node{myNode}}); err == nil {
						nodes = append(nodes, list.Node...)
					} else {
						n.nodeUpdate <- hashring.NodeUpdate{
							Node:   myNode,
							Status: hashring.Offline,
						}
					}
					cancel()
				}
			}
			for _, v := range nodes {
				n.nodeUpdate <- hashring.NodeUpdate{
					Node:   v,
					Status: hashring.Online,
				}
			}
		}
	}
}

func (n *NodeImpl) Shutdown() {
	alreadyShutdown := n.isShutdown.Swap(true)
	if alreadyShutdown {
		log.Println("Node is already shutting down")
		return
	}

	close(n.done) // Signal loops to stop

	n.nodeLock.Lock()

	self := &proto.Node{Host: n.self.Addr().String(), Port: uint32(n.self.Port())}
	delete(n.peerList, n.selfHash)
	var wg sync.WaitGroup
	for k, p := range n.peerList {
		if k != n.selfHash {
			wg.Add(1)
			myPeer := p
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*40)
				myPeer.NodeStatusClient.RemoveNode(ctx, self)
				defer cancel()
				defer wg.Done()
			}()
		}
	}
	n.nodeLock.Unlock()
	wg.Wait()

	n.tryRebalance()
}

func (n *NodeImpl) tryRebalance() {
	n.nodeLock.RLock()
	nodesRemaining := false
	for range n.peerList {
		nodesRemaining = true
		break
	}
	n.nodeLock.RUnlock()
	if nodesRemaining {
		n.rebalancer(n)
	} else {
		log.Println("Last node in the network is gone!")
	}
}

func CreateClient(server netip.AddrPort) (*Client, error) {
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(server.String(), opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
		return nil, err
	}
	//defer conn.Close()
	return &Client{conn, proto.NewHashStoreClient(conn), proto.NewNodeStatusClient(conn), server}, nil

}
