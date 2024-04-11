package internal

import (
	"context"
	"log"
	"time"

	"github.com/ReneKroon/hashring"
	"github.com/ReneKroon/hashring/proto"
)

type ServerKeyImpl struct {
	//store map[string]string
	localData map[string]string
}

func NewKeyImpl() hashring.ServerKey {
	return ServerKeyImpl{make(map[string]string)}
}

func (s ServerKeyImpl) Get(key string) (*string, bool) {

	data, found := s.localData[key]

	if found {
		return &data, found
	}
	return nil, found

}
func (s ServerKeyImpl) Put(key string, data string) {

	s.localData[key] = data

}
func (s ServerKeyImpl) Remove(key string) {
	delete(s.localData, key)
}

// after a node is being shutdown, or this node is shutting down it should rebalance the keys over the network
func (s ServerKeyImpl) Rebalance(node hashring.Node) {
	log.Println("Rebalance!")
	for k, v := range s.localData {
		if client, self := node.GetNode(k); !self {
			data := &proto.KeyData{Key: k, Data: v}
			delete(s.localData, k)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				client.Put(ctx, data)
			}()
		}

	}

}
