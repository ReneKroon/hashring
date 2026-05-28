package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
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
}

func NewResilientClient(initialServer string) (*ResilientClient, error) {
	rc := &ResilientClient{
		connections: make(map[string]*grpc.ClientConn),
		servers:     []string{},
	}

	// Connect to initial server
	if err := rc.addServer(initialServer); err != nil {
		return nil, fmt.Errorf("failed to connect to initial server: %w", err)
	}

	return rc, nil
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
	log.Printf("Connected to server: %s", server)
	return nil
}

func (rc *ResilientClient) removeServer(server string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if conn, exists := rc.connections[server]; exists {
		conn.Close()
		delete(rc.connections, server)

		// Remove from servers list
		for i, s := range rc.servers {
			if s == server {
				rc.servers = append(rc.servers[:i], rc.servers[i+1:]...)
				break
			}
		}
		log.Printf("Disconnected from server: %s", server)
	}
}

func (rc *ResilientClient) getRandomClient() (proto.HashStoreClient, string, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if len(rc.servers) == 0 {
		return nil, "", fmt.Errorf("no servers available")
	}

	// Pick a random server
	server := rc.servers[rand.IntN(len(rc.servers))]
	conn := rc.connections[server]
	return proto.NewHashStoreClient(conn), server, nil
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
		if !currentServers[server] {
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
	return retryOperation(ctx, rc, func(client proto.HashStoreClient) (*proto.Data, error) {
		return client.Get(ctx, key)
	})
}

func (rc *ResilientClient) Put(ctx context.Context, keyData *proto.KeyData) (*proto.UpdateStatus, error) {
	return retryOperation(ctx, rc, func(client proto.HashStoreClient) (*proto.UpdateStatus, error) {
		return client.Put(ctx, keyData)
	})
}

func (rc *ResilientClient) Remove(ctx context.Context, key *proto.Key) (*proto.UpdateStatus, error) {
	return retryOperation(ctx, rc, func(client proto.HashStoreClient) (*proto.UpdateStatus, error) {
		return client.Remove(ctx, key)
	})
}

// retryOperation tries the operation on different servers until success or all servers fail
func retryOperation[T any](_ context.Context, rc *ResilientClient, operation func(proto.HashStoreClient) (T, error)) (T, error) {
	var lastErr error
	var zero T

	rc.mu.RLock()
	serverCount := len(rc.servers)
	rc.mu.RUnlock()

	// Try up to the number of available servers
	for attempt := range serverCount {
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

	initialServer := fmt.Sprintf("%s:%d", internal.GetLocalIP(), *port)

	client, err := NewResilientClient(initialServer)
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
		time.Sleep(5 * time.Second)

		// Try to get node list from any available server
		clients := rc.getAllNodeStatusClients()
		if len(clients) == 0 {
			log.Println("No servers available to query node list")
			continue
		}

		var nodeList *proto.NodeList
		var err error

		// Try each server until we get a successful response
		for _, c := range clients {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			nodeList, err = c.client.GetNodeList(ctx, &proto.NodeList{})
			cancel()

			if err == nil {
				break
			}
			log.Printf("Failed to get node list from %s: %v", c.server, err)
		}

		if err != nil {
			log.Println("Failed to get node list from all servers")
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
