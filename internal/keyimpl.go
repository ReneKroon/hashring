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

func (s *ServerKeyImpl) PutMany(data []*proto.KeyData) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for _, d := range data {

		s.LocalData[d.Key] = d.Data
	}

}

func (s *ServerKeyImpl) Remove(key string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.LocalData, key)
}

// after a node is being shutdown, or this node is shutting down it should rebalance the keys over the network
func (s *ServerKeyImpl) Rebalance(node hashring.Node) (kept, sent int) {
	log.Println("Rebalance!")

	var newDistribution = make(map[proto.HashStoreClient][]*proto.KeyData)

	s.mutex.Lock()
	startCount := len(s.LocalData)
	for k, v := range s.LocalData {
		if client, self := node.GetNode(k); !self {
			sent++

			data := &proto.KeyData{Key: k, Data: v}
			delete(s.LocalData, k)

			newDistribution[client] = append(newDistribution[client], data)
		} else {
			kept++
		}

	}

	for client, data := range newDistribution {
		if client.(*Client) == nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if status, err := client.PutMany(ctx, &proto.KeyDataList{KeyData: data}); err != nil || !status.Ok {
			log.Println("Error rebalancing keys to node", client, ":", err)
			for _, v := range data {
				log.Println("Rebalancing key", v.Key, " failed, putting it back")

				s.LocalData[v.Key] = v.Data // put it back
			}
		}

	}

	s.mutex.Unlock()
	log.Printf("%d start, %d kept, %d sent", startCount, kept, sent)
	return kept, sent
}
