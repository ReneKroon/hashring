package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"github.com/ReneKroon/hashring/internal"
	"github.com/ReneKroon/hashring/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var port = flag.Int("port", 7070, "Set the port to listen to for this server")

// ResilientClient maintains connections to multiple servers with automatic failover
type ResilientClient struct {
	mu          sync.RWMutex
	connections map[string]*grpc.ClientConn
	servers     []string
	hasher      internal.HashCrc32
	ring        map[uint32]string // vnode hash -> server address
	vnodeHashes []uint32          // all vnode hashes, passed to Hasher.GetNodeForHash
	// seeds are bootstrap servers supplied at construction. They are pinned
	// in the connection pool so a gossip response that happens to omit them
	// (e.g. queried peer doesn't know about a momentarily dead seed) doesn't
	// drop our only way back into the cluster. Read-only after construction.
	seeds map[string]bool
	// online tracks per-server reachability based on the periodic health
	// probe in monitorNodes. Only online servers contribute vnodes to
	// `ring`/`vnodeHashes`, so consistent-hash lookups naturally route
	// around dead nodes until they recover.
	online map[string]bool
}

func NewResilientClient(initialServers []string) (*ResilientClient, error) {
	if len(initialServers) == 0 {
		return nil, fmt.Errorf("at least one seed server is required")
	}
	rc := &ResilientClient{
		connections: make(map[string]*grpc.ClientConn),
		servers:     []string{},
		hasher:      internal.HashCrc32{},
		ring:        make(map[uint32]string),
		vnodeHashes: []uint32{},
		seeds:       make(map[string]bool, len(initialServers)),
		online:      make(map[string]bool),
	}

	for _, s := range initialServers {
		if rc.seeds[s] {
			continue
		}
		rc.seeds[s] = true
		if err := rc.addServer(s); err != nil {
			return nil, fmt.Errorf("failed to connect to seed %s: %w", s, err)
		}
	}

	return rc, nil
}

const VNODE_COUNT = 20

func (rc *ResilientClient) getVirtualHashes(server string) []uint32 {
	hashes := make([]uint32, VNODE_COUNT)
	for i := range VNODE_COUNT {
		hashes[i] = rc.hasher.HashString(fmt.Sprintf("%s#%d", server, i))
	}
	return hashes
}

// rebuildRing rebuilds the consistent-hash ring from servers currently
// marked online. Offline servers are skipped so their key range fails over
// to the next online vnode in the ring. Callers must hold rc.mu.
func (rc *ResilientClient) rebuildRing() {
	rc.ring = make(map[uint32]string)
	rc.vnodeHashes = rc.vnodeHashes[:0]

	for _, server := range rc.servers {
		// Exclude only servers the health probe has explicitly marked
		// offline. Absent entries (e.g. just-added or never-probed) are
		// presumed reachable; the next probe cycle corrects the state.
		if online, set := rc.online[server]; set && !online {
			continue
		}
		for _, hash := range rc.getVirtualHashes(server) {
			rc.ring[hash] = server
			rc.vnodeHashes = append(rc.vnodeHashes, hash)
		}
	}
	// Keep vnodeHashes sorted ascending so getClientForKey can call the
	// shared Hasher.GetNodeForHash, which requires sorted input.
	slices.Sort(rc.vnodeHashes)
}

func (rc *ResilientClient) addServer(server string) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Check if already connected
	if _, exists := rc.connections[server]; exists {
		return nil
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(server, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", server, err)
	}

	rc.connections[server] = conn
	rc.servers = append(rc.servers, server)
	// Assume reachable until the next health probe says otherwise. Routing
	// to a freshly-added but actually-dead seed costs one failed request
	// before the probe corrects the state.
	rc.online[server] = true
	rc.rebuildRing()
	log.Printf("Connected to server: %s", server)
	return nil
}

func (rc *ResilientClient) removeServer(server string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if conn, exists := rc.connections[server]; exists {
		conn.Close()
		delete(rc.connections, server)
		delete(rc.online, server)

		// Remove from servers list
		for i, s := range rc.servers {
			if s == server {
				rc.servers = append(rc.servers[:i], rc.servers[i+1:]...)
				break
			}
		}
		rc.rebuildRing()
		log.Printf("Disconnected from server: %s", server)
	}
}

// getClientForKey finds the server responsible for a key using consistent hashing
func (rc *ResilientClient) getClientForKey(key string) (proto.HashStoreClient, string, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	// vnodeHashes only contains entries for online servers; if it's empty,
	// every known server is currently marked offline.
	if len(rc.vnodeHashes) == 0 {
		return nil, "", fmt.Errorf("no online servers available")
	}

	// Hash the key and pick the owning vnode using the shared selection
	// function on the Hasher interface (hash.go). Going through the same
	// implementation as the server (internal/hashimpl.go:getNodeForHash)
	// guarantees the client and server agree on the owner, so requests
	// land directly on the correct node instead of being forwarded.
	keyHash := rc.hasher.HashString(key)
	vnodeHash, _ := rc.hasher.GetNodeForHash(keyHash, rc.vnodeHashes, 0)
	server := rc.ring[vnodeHash]
	conn := rc.connections[server]

	return proto.NewHashStoreClient(conn), server, nil
}

func (rc *ResilientClient) getRandomClient() (proto.HashStoreClient, string, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	// Restrict the retry pool to servers the health probe hasn't marked
	// offline; falling back onto a known-dead server just wastes a timeout.
	candidates := make([]string, 0, len(rc.servers))
	for _, s := range rc.servers {
		if online, set := rc.online[s]; set && !online {
			continue
		}
		candidates = append(candidates, s)
	}
	if len(candidates) == 0 {
		return nil, "", fmt.Errorf("no online servers available")
	}
	server := candidates[rand.IntN(len(candidates))]
	conn := rc.connections[server]
	return proto.NewHashStoreClient(conn), server, nil
}

// setOnline updates a server's reachability state and rebuilds the ring
// if it actually changed. Safe to call from the health probe goroutine.
func (rc *ResilientClient) setOnline(server string, isOnline bool) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Ignore updates for servers we've since dropped from the pool.
	if _, known := rc.connections[server]; !known {
		return
	}
	prev, exists := rc.online[server]
	if exists && prev == isOnline {
		return
	}
	rc.online[server] = isOnline
	rc.rebuildRing()
	if isOnline {
		log.Printf("Server %s back online", server)
	} else {
		log.Printf("Server %s marked offline", server)
	}
}

func (rc *ResilientClient) getAllNodeStatusClients() []struct {
	client proto.NodeStatusClient
	server string
} {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	result := make([]struct {
		client proto.NodeStatusClient
		server string
	}, 0, len(rc.servers))

	for _, server := range rc.servers {
		conn := rc.connections[server]
		result = append(result, struct {
			client proto.NodeStatusClient
			server string
		}{proto.NewNodeStatusClient(conn), server})
	}

	return result
}

func (rc *ResilientClient) UpdateServerList(nodeList *proto.NodeList) {
	currentServers := make(map[string]bool)

	for _, node := range nodeList.Node {
		server := fmt.Sprintf("%s:%d", node.Host, node.Port)
		currentServers[server] = true

		// Add new servers
		if err := rc.addServer(server); err != nil {
			log.Printf("Failed to add server %s: %v", server, err)
		}
	}

	// Remove servers that are no longer in the list
	rc.mu.RLock()
	existingServers := make([]string, len(rc.servers))
	copy(existingServers, rc.servers)
	rc.mu.RUnlock()

	for _, server := range existingServers {
		// Keep seeds in the pool even if the latest gossip response omits
		// them — they're the fallback path for when other peers go away.
		if !currentServers[server] && !rc.seeds[server] {
			rc.removeServer(server)
		}
	}
}

func (rc *ResilientClient) Close() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	for _, conn := range rc.connections {
		conn.Close()
	}
	rc.connections = make(map[string]*grpc.ClientConn)
	rc.servers = []string{}
}

// Resilient operations with automatic retry
func (rc *ResilientClient) Get(ctx context.Context, key *proto.Key) (*proto.Data, error) {
	return retryOperationWithKey(ctx, rc, key.Key, func(client proto.HashStoreClient) (*proto.Data, error) {
		return client.Get(ctx, key)
	})
}

func (rc *ResilientClient) Put(ctx context.Context, keyData *proto.KeyData) (*proto.UpdateStatus, error) {
	return retryOperationWithKey(ctx, rc, keyData.Key, func(client proto.HashStoreClient) (*proto.UpdateStatus, error) {
		return client.Put(ctx, keyData)
	})
}

func (rc *ResilientClient) Remove(ctx context.Context, key *proto.Key) (*proto.UpdateStatus, error) {
	return retryOperationWithKey(ctx, rc, key.Key, func(client proto.HashStoreClient) (*proto.UpdateStatus, error) {
		return client.Remove(ctx, key)
	})
}

// retryOperationWithKey tries the operation on the correct server first (based on key hash), then falls back to others
func retryOperationWithKey[T any](_ context.Context, rc *ResilientClient, key string, operation func(proto.HashStoreClient) (T, error)) (T, error) {
	var lastErr error
	var zero T

	rc.mu.RLock()
	serverCount := len(rc.servers)
	rc.mu.RUnlock()

	// First try: use consistent hashing to find the right server
	client, server, err := rc.getClientForKey(key)
	if err != nil {
		return zero, err
	}

	result, err := operation(client)
	if err == nil {
		return result, nil
	}

	lastErr = err
	log.Printf("Primary server %s failed for key %s: %v", server, key, err)

	// Retry on other servers if primary fails
	for attempt := 1; attempt < serverCount; attempt++ {
		client, server, err := rc.getRandomClient()
		if err != nil {
			return zero, err
		}

		result, err := operation(client)
		if err == nil {
			return result, nil
		}

		lastErr = err
		log.Printf("Attempt %d/%d failed on server %s: %v", attempt+1, serverCount, server, err)

		// Small delay before retry (except on last attempt)
		if attempt < serverCount-1 {
			time.Sleep(50 * time.Millisecond)
		}
	}

	return zero, fmt.Errorf("all servers failed, last error: %w", lastErr)
}

func main() {
	flag.Parse()

	// Always include the conventional bootstrap peer (port 7070) alongside
	// the -port flag, mirroring the server (cmd/server/server.go) which
	// seeds itself with 7070 regardless of its own port. This gives the
	// client a second starter node so it can still reach the cluster when
	// 7070 is down at startup.
	localIP := internal.GetLocalIP()
	seeds := []string{
		fmt.Sprintf("%s:%d", localIP, 7070),
		fmt.Sprintf("%s:%d", localIP, *port),
	}

	client, err := NewResilientClient(seeds)
	if err != nil {
		log.Fatalf("Failed to create resilient client: %v", err)
	}
	defer client.Close()

	// Start background goroutine to monitor node updates
	go monitorNodes(client)

	for {
		for i := range 100 {
			key := fmt.Sprintf("test%d", i)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			if _, err := client.Put(ctx, &proto.KeyData{Key: key, Data: "data"}); err != nil {
				log.Printf("Put failed for key %s: %v", key, err)
			}
			cancel()

			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			if data, err := client.Get(ctx, &proto.Key{Key: key}); err == nil {
				if !data.Found {
					fmt.Println("Not found")
				}
			} else {
				fmt.Println("->" + err.Error())
			}
			cancel()

			time.Sleep(time.Millisecond * 150)
		}

		for i := range 100 {
			key := fmt.Sprintf("test%d", i)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			if data, err := client.Get(ctx, &proto.Key{Key: key}); err == nil {
				if !data.Found {
					fmt.Println("Not found")
				}
			} else {
				fmt.Println(err)
			}
			cancel()

			ctx, cancel = context.WithTimeout(context.Background(), time.Second)
			if _, err := client.Remove(ctx, &proto.Key{Key: key}); err != nil {
				log.Printf("Remove failed for key %s: %v", key, err)
			}
			cancel()
		}
	}
}

func monitorNodes(rc *ResilientClient) {
	var previousNodes map[string]bool

	for {
		time.Sleep(2 * time.Second)

		// Probe every server in the pool. A short per-request timeout keeps
		// detection latency bounded when a node is unreachable; the result
		// doubles as both a health signal (setOnline) and, on success,
		// fresh peer-discovery input for UpdateServerList.
		clients := rc.getAllNodeStatusClients()
		if len(clients) == 0 {
			log.Println("No servers available to query node list")
			continue
		}

		var nodeList *proto.NodeList
		anySuccess := false

		for _, c := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			list, err := c.client.GetNodeList(ctx, &proto.NodeList{})
			cancel()

			if err != nil {
				rc.setOnline(c.server, false)
				continue
			}
			rc.setOnline(c.server, true)
			if !anySuccess {
				// Any single peer's view is good enough for peer discovery;
				// the first successful response wins this tick.
				nodeList = list
				anySuccess = true
			}
		}

		if !anySuccess {
			log.Println("All servers unreachable this tick")
			continue
		}

		// Update the connection pool with the new server list
		rc.UpdateServerList(nodeList)

		currentNodes := make(map[string]bool)
		for _, node := range nodeList.Node {
			nodeAddr := fmt.Sprintf("%s:%d", node.Host, node.Port)
			currentNodes[nodeAddr] = true

			// Check if this is a new node
			if len(previousNodes) > 0 && !previousNodes[nodeAddr] {
				fmt.Printf("NEW NODE: %s\n", nodeAddr)
			}
		}

		// Check for removed nodes
		for nodeAddr := range previousNodes {
			if !currentNodes[nodeAddr] {
				fmt.Printf("REMOVED NODE: %s\n", nodeAddr)
			}
		}

		if len(previousNodes) == 0 {
			fmt.Printf("Initial cluster topology (%d nodes):\n", len(currentNodes))
			for nodeAddr := range currentNodes {
				fmt.Printf("  - %s\n", nodeAddr)
			}
		}

		previousNodes = currentNodes
	}
}
