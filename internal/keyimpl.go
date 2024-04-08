package internal

import (
	"github.com/ReneKroon/hashring"
)

type ServerKeyImpl struct {
	//store map[string]string
	localData map[string]string
}

func NewKeyImpl() hashring.ServerKey {
	return ServerKeyImpl{make(map[string]string)}
}

func (k ServerKeyImpl) Get(key string) (*string, bool) {

	data, found := k.localData[key]

	if found {
		return &data, found
	}
	return nil, found

}
func (k ServerKeyImpl) Put(key string, data string) {

	k.localData[key] = data

}
func (k ServerKeyImpl) Remove(key string) {
	delete(k.localData, key)
}
