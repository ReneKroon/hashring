package hashring

import "github.com/ReneKroon/hashring/proto"

type Ring interface {
	Node
	Hasher
	proto.HashStoreServer
}
