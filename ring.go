package hashring

import "github.com/ReneKroon/hashring/proto"

type Ring interface {
	Node
	//ClientKey
	Hasher
	proto.HashStoreServer
}
