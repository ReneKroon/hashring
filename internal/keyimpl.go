package internal

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/proto"
)

type ServerKeyImpl struct {
	//safe for debugging only
	LocalData map[string]string
	mutex     sync.RWMutex
}

func NewServerKeyImpl() hashring.ServerKey {
	return &ServerKeyImpl{make(map[string]string), sync.RWMutex{}}
}

func (s *ServerKeyImpl) Get(key string) (string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	data, found := s.LocalData[key]

	if found {
		return data, found
	}
	return "", found

}
func (s *ServerKeyImpl) Put(key string, data string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.LocalData[key] = data

}
func (s *ServerKeyImpl) Remove(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.LocalData, key)
}

// after a node is being shutdown, or this node is shutting down it should rebalance the keys over the network
func (s *ServerKeyImpl) Rebalance(node hashring.Node) (kept, sent int) {
	log.Println("Rebalance!")

	for k, v := range s.LocalData {
		if client, self := node.GetNode(k); !self {
			sent++
			s.mutex.Lock()
			data := &proto.KeyData{Key: k, Data: v}
			delete(s.LocalData, k)
			s.mutex.Unlock()
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if client.(*Client) != nil {
					client.Put(ctx, data)
				}
			}()
		} else {
			kept++
		}

	}
	log.Printf("%d kept, %d sent", kept, sent)
	return kept, sent
}
