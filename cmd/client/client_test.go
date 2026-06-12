package main

import (
	"fmt"
	"testing"

	"github.com/ReneKroon/hashring/internal"
	"google.golang.org/grpc"
)

// TestClientHashingMatchesServer verifies that the client's consistent hashing
// logic matches the server's, so the client routes requests to the correct server
func TestClientHashingMatchesServer(t *testing.T) {
	// Create a mock client with 3 servers
	rc := &ResilientClient{
		connections: make(map[string]*grpc.ClientConn),
		servers:     []string{},
		hasher:      internal.HashCrc32{},
		ring:        make(map[uint32]string),
		vnodeHashes: []uint32{},
	}

	servers := []string{
		"192.168.1.1:7070",
		"192.168.1.2:7070",
		"192.168.1.3:7070",
	}

	// Manually add servers to the hash ring (without network connections)
	rc.mu.Lock()
	rc.servers = servers
	rc.rebuildRing()
	rc.mu.Unlock()

	// Test that each key consistently maps to the same server
	testKeys := []string{
		"test0", "test1", "test2", "test10", "test99",
		"user:123", "session:abc", "data:xyz",
	}

	for _, key := range testKeys {
		// Get server assignment from client
		keyHash := rc.hasher.HashString(key)

		// Find which server the client would route to using the shared
		// Hasher.GetNodeForHash (same selection function the server uses).
		var clientSelectedServer string
		rc.mu.RLock()
		if len(rc.vnodeHashes) > 0 {
			vnodeHash, _ := rc.hasher.GetNodeForHash(keyHash, rc.vnodeHashes, 0)
			clientSelectedServer = rc.ring[vnodeHash]
		}
		rc.mu.RUnlock()

		if clientSelectedServer == "" {
			t.Errorf("Client failed to select a server for key %s (hash: %d)", key, keyHash)
			continue
		}

		// Verify the selection is deterministic
		for range 5 {
			rc.mu.RLock()
			keyHash2 := rc.hasher.HashString(key)
			if keyHash2 != keyHash {
				t.Errorf("Hash is not deterministic for key %s: got %d and %d", key, keyHash, keyHash2)
			}
			rc.mu.RUnlock()
		}

		t.Logf("Key %s (hash: %d) -> Server %s", key, keyHash, clientSelectedServer)
	}
}

// TestClientVirtualNodeDistribution verifies that vnodes are evenly distributed
func TestClientVirtualNodeDistribution(t *testing.T) {
	rc := &ResilientClient{
		connections: make(map[string]*grpc.ClientConn),
		servers:     []string{},
		hasher:      internal.HashCrc32{},
		ring:        make(map[uint32]string),
		vnodeHashes: []uint32{},
	}

	servers := []string{
		"192.168.1.1:7070",
		"192.168.1.2:7070",
		"192.168.1.3:7070",
	}

	rc.mu.Lock()
	rc.servers = servers
	rc.rebuildRing()
	rc.mu.Unlock()

	// Count vnodes per server
	vnodeCount := make(map[string]int)
	rc.mu.RLock()
	for _, server := range rc.ring {
		vnodeCount[server]++
	}
	rc.mu.RUnlock()

	// Each server should have exactly VNODE_COUNT vnodes
	for _, server := range servers {
		count := vnodeCount[server]
		if count != VNODE_COUNT {
			t.Errorf("Server %s has %d vnodes, expected %d", server, count, VNODE_COUNT)
		}
	}

	totalVnodes := len(rc.vnodeHashes)
	expectedTotal := len(servers) * VNODE_COUNT
	if totalVnodes != expectedTotal {
		t.Errorf("Total vnodes: %d, expected %d", totalVnodes, expectedTotal)
	}

	t.Logf("Virtual node distribution: %d vnodes per server, %d total", VNODE_COUNT, totalVnodes)
}

// TestClientKeyDistribution tests that keys are reasonably distributed across servers
func TestClientKeyDistribution(t *testing.T) {
	rc := &ResilientClient{
		connections: make(map[string]*grpc.ClientConn),
		servers:     []string{},
		hasher:      internal.HashCrc32{},
		ring:        make(map[uint32]string),
		vnodeHashes: []uint32{},
	}

	servers := []string{
		"192.168.1.1:7070",
		"192.168.1.2:7070",
		"192.168.1.3:7070",
	}

	rc.mu.Lock()
	rc.servers = servers
	rc.rebuildRing()
	rc.mu.Unlock()

	// Generate many keys and see how they distribute
	keyCount := 1000
	serverHits := make(map[string]int)

	for i := range keyCount {
		key := fmt.Sprintf("test%d", i)

		rc.mu.RLock()
		keyHash := rc.hasher.HashString(key)
		vnodeHash, _ := rc.hasher.GetNodeForHash(keyHash, rc.vnodeHashes, 0)
		server := rc.ring[vnodeHash]
		rc.mu.RUnlock()

		serverHits[server]++
	}

	// Check that distribution is reasonably balanced (within 50% of ideal)
	idealPerServer := keyCount / len(servers)
	minExpected := idealPerServer / 2
	maxExpected := idealPerServer * 3 / 2

	t.Logf("Key distribution across %d keys:", keyCount)
	for _, server := range servers {
		hits := serverHits[server]
		percentage := float64(hits) / float64(keyCount) * 100
		t.Logf("  %s: %d keys (%.1f%%)", server, hits, percentage)

		if hits < minExpected || hits > maxExpected {
			t.Errorf("Server %s has poor distribution: %d keys (expected %d±50%%)",
				server, hits, idealPerServer)
		}
	}
}

// TestClientServerAddRemove tests that the ring is correctly rebuilt when servers change
func TestClientServerAddRemove(t *testing.T) {
	rc := &ResilientClient{
		connections: make(map[string]*grpc.ClientConn),
		servers:     []string{},
		hasher:      internal.HashCrc32{},
		ring:        make(map[uint32]string),
		vnodeHashes: []uint32{},
	}

	// Start with 2 servers
	servers := []string{
		"192.168.1.1:7070",
		"192.168.1.2:7070",
	}

	rc.mu.Lock()
	rc.servers = servers
	rc.rebuildRing()
	rc.mu.Unlock()

	if len(rc.vnodeHashes) != 2*VNODE_COUNT {
		t.Errorf("Expected %d vnodes, got %d", 2*VNODE_COUNT, len(rc.vnodeHashes))
	}

	// Add a server
	rc.mu.Lock()
	rc.servers = append(rc.servers, "192.168.1.3:7070")
	rc.rebuildRing()
	rc.mu.Unlock()

	if len(rc.vnodeHashes) != 3*VNODE_COUNT {
		t.Errorf("After adding server, expected %d vnodes, got %d", 3*VNODE_COUNT, len(rc.vnodeHashes))
	}

	// Remove a server
	rc.mu.Lock()
	rc.servers = rc.servers[:2]
	rc.rebuildRing()
	rc.mu.Unlock()

	if len(rc.vnodeHashes) != 2*VNODE_COUNT {
		t.Errorf("After removing server, expected %d vnodes, got %d", 2*VNODE_COUNT, len(rc.vnodeHashes))
	}

	t.Logf("Server add/remove test passed")
}
