package hashring

type Ring interface {
	Node
	ClientKey
	Hasher
}
