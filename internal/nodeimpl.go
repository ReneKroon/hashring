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

const VNODE_COUNT = 20

type NodeImpl struct {
	hashring.Hasher
	// peerList should include 'self' with a nil value
	peerList map[uint32]*Client
	// list of nodes that are not reachable right now
	offlineList map[uint32]*Client
	self        netip.AddrPort
	selfHashes  map[uint32]bool
	proto.UnimplementedNodeStatusServer
	rebalancer   func(hashring.Node) (int, int)
	createclient func(server netip.AddrPort) (*Client, error)
	nodeLock     sync.RWMutex
	nodeUpdate   chan (hashring.NodeUpdate)
	done         chan struct{}
	isShutdown   atomic.Bool
}

func (n *NodeImpl) getVirtualHashes(peer netip.AddrPort) []uint32 {
	hashes := make([]uint32, VNODE_COUNT)
	for i := 0; i < VNODE_COUNT; i++ {
		hashes[i] = n.HashString(fmt.Sprintf("%s#%d", peer.String(), i))
	}
	return hashes
}

func NewNodeImpl(inital []netip.AddrPort, self netip.AddrPort, h hashring.Hasher, k hashring.ServerKey, createclient func(server netip.AddrPort) (*Client, error)) hashring.Node {
	p := map[uint32]*Client{}

	nImpl := &NodeImpl{
		Hasher:                        h,
		peerList:                      p,
		self:                          self,
		selfHashes:                    make(map[uint32]bool),
		UnimplementedNodeStatusServer: proto.UnimplementedNodeStatusServer{},
		rebalancer:                    k.Rebalance,
		createclient:                  createclient,
		nodeLock:                      sync.RWMutex{},
		nodeUpdate:                    make(chan hashring.NodeUpdate, 100),
		done:                          make(chan struct{}),
		isShutdown:                    atomic.Bool{},
	}

	for _, v := range nImpl.getVirtualHashes(self) {
		p[v] = nil
		nImpl.selfHashes[v] = true
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
			for _, hash := range nImpl.getVirtualHashes(v) {
				nImpl.peerList[hash] = nil
			}
		}
	}

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
	hashes := n.getVirtualHashes(peer)
	_, found := n.peerList[hashes[0]]
	if !found {
		log.Println("Add a node ", node.Node.Host, node.Node.Port)

		if client, err := n.createclient(peer); err == nil {
			for _, hash := range hashes {
				n.peerList[hash] = client
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			client.AddNode(ctx, &proto.Node{Host: n.self.Addr().String(), Port: uint32(n.self.Port())})
			cancel()
		} else {
			log.Println("Error creating client for node", peer, ":", err)
		}
	}
}

func (n *NodeImpl) removeNode(peer netip.AddrPort, node hashring.NodeUpdate) {
	hashes := n.getVirtualHashes(peer)
	client, found := n.peerList[hashes[0]]
	if found {
		for _, hash := range hashes {
			delete(n.peerList, hash)
		}
		if n.offlineList == nil {
			n.offlineList = make(map[uint32]*Client)
		}
		n.offlineList[hashes[0]] = client
	}
}

func (n *NodeImpl) AddNode(ctx context.Context, node *proto.Node) (*emptypb.Empty, error) {

	n.nodeUpdate <- hashring.NodeUpdate{Node: node, Status: hashring.Online}

	return &emptypb.Empty{}, nil
}

func (n *NodeImpl) GetNodeList(ctx context.Context, newNodes *proto.NodeList) (*proto.NodeList, error) {
	n.nodeLock.RLock()
	defer n.nodeLock.RUnlock()

	uniqueNodes := make(map[netip.AddrPort]bool)
	list := &proto.NodeList{}
	for _, v := range n.peerList {
		if v == nil {
			continue
		}
		if _, ok := uniqueNodes[v.AddrPort]; !ok {
			uniqueNodes[v.AddrPort] = true
			list.Node = append(list.Node, &proto.Node{Host: v.Addr().String(), Port: uint32(v.Port())})
		}
	}
	list.Node = append(list.Node, &proto.Node{Host: n.self.Addr().String(), Port: uint32(n.self.Port())})
	return list, nil
}

func (n *NodeImpl) RemoveNode(ctx context.Context, node *proto.Node) (*emptypb.Empty, error) {
	n.nodeLock.Lock()

	if peer, err := netip.ParseAddrPort(fmt.Sprintf("%s:%d", node.Host, node.Port)); err == nil {
		log.Println("Removing a node ", peer.String())
		hashes := n.getVirtualHashes(peer)
		if v, ok := n.peerList[hashes[0]]; ok {
			for _, hash := range hashes {
				delete(n.peerList, hash)
			}

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
	hash, _ := n.GetNodeForHash(crc32, peerList, 0) // Passing 0 for selfHash as we check selfHashes instead

	if n.selfHashes[hash] {
		return nil, true
	} else {
		return n.peerList[hash], false
	}
}

const MAX_GOSSIP_PEERS = 3

func (n *NodeImpl) findNeighbours(done chan struct{}) {
	for {
		delay := 10 + rand.IntN(10)
		timer := time.NewTimer(time.Second * time.Duration(delay))
		select {
		case <-done:
			timer.Stop()
			return
		case <-timer.C:
			var uniquePeers []*Client
			n.nodeLock.RLock()
			peerMap := make(map[*Client]bool)
			for _, p := range n.peerList {
				if p != nil && !peerMap[p] {
					peerMap[p] = true
					uniquePeers = append(uniquePeers, p)
				}
			}
			n.nodeLock.RUnlock()

			if len(uniquePeers) == 0 {
				continue
			}

			// Randomly shuffle and pick a subset
			rand.Shuffle(len(uniquePeers), func(i, j int) {
				uniquePeers[i], uniquePeers[j] = uniquePeers[j], uniquePeers[i]
			})

			gossipPeers := uniquePeers
			if len(gossipPeers) > MAX_GOSSIP_PEERS {
				gossipPeers = gossipPeers[:MAX_GOSSIP_PEERS]
			}

			nodesDiscovered := []*proto.Node{}
			myNode := &proto.Node{Host: n.self.Addr().String(), Port: uint32(n.self.Port())}

			for _, p := range gossipPeers {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
				if list, err := p.GetNodeList(ctx, &proto.NodeList{Node: []*proto.Node{myNode}}); err == nil {
					nodesDiscovered = append(nodesDiscovered, list.Node...)
				} else {
					log.Println("Error gossiping with peer", p.AddrPort, ":", err)
					// If a peer fails gossip, we could mark it as offline, 
					// but let's be conservative for now and just log it.
				}
				cancel()
			}

			for _, v := range nodesDiscovered {
				// Only update if it's a new node to us
				if peer, err := netip.ParseAddrPort(fmt.Sprintf("%s:%d", v.Host, v.Port)); err == nil && peer != n.self {
					n.nodeLock.RLock()
					hashes := n.getVirtualHashes(peer)
					_, found := n.peerList[hashes[0]]
					n.nodeLock.RUnlock()

					if !found {
						n.nodeUpdate <- hashring.NodeUpdate{
							Node:   v,
							Status: hashring.Online,
						}
					}
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
	for _, hash := range n.getVirtualHashes(n.self) {
		delete(n.peerList, hash)
	}

	var wg sync.WaitGroup
	uniqueClients := make(map[*Client]bool)
	for _, p := range n.peerList {
		if p != nil && !uniqueClients[p] {
			uniqueClients[p] = true
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
