package structure

import (
	"baldb/funcp"
	"hash/fnv"
	"strings"
)

type Node struct {
	Key   string
	Hash  string
	Value float64
}

func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (n *Node) ComputeHash() string {
	return funcp.Reduce(Hash, strings.Split(n.Key, ":"))
}

func (n *Node) IsPartOfHashKey(hash string) bool {
	return strings.Contains(n.Key, hash) || strings.Contains(hash, n.Key)
}

func (n *Node) IsSomePartOfHashKey(hash string) bool {
	return funcp.Find(strings.Split(n.Key, ":"), hash)
}
