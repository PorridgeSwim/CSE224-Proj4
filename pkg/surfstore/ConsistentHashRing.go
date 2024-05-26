package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap  map[string]string
	SortedKeys []string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	blockHash := blockId
	// blockHash := c.Hash(blockId)
	for _, key := range c.SortedKeys {
		if key > blockHash {
			return c.ServerMap[key]
		}
	}
	return c.ServerMap[c.SortedKeys[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	c := &ConsistentHashRing{
		ServerMap:  make(map[string]string),
		SortedKeys: []string{},
	}
	for _, addr := range serverAddrs {
		c.ServerMap[c.Hash("blockstore"+addr)] = addr
	}
	for key := range c.ServerMap {
		c.SortedKeys = append(c.SortedKeys, key)
	}
	sort.Strings(c.SortedKeys)
	return c
}
