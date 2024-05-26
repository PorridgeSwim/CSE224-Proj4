package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	if block, ok := bs.BlockMap[blockHash.GetHash()]; ok {
		return block, nil
	} else {
		return nil, fmt.Errorf("Block with hash %s not found", blockHash.GetHash())
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	index := GetBlockHashString(block.GetBlockData())
	bs.BlockMap[index] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// hashes that are not stored in the key-value store
func (bs *BlockStore) MissingBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	blockHashesOut := &BlockHashes{}
	for _, hash := range blockHashesIn.GetHashes() {
		if _, ok := bs.BlockMap[hash]; !ok {
			blockHashesOut.Hashes = append(blockHashesOut.Hashes, hash)
		}
	}
	return blockHashesOut, nil
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	allBlockHashes := &BlockHashes{}
	for hash := range bs.BlockMap {
		allBlockHashes.Hashes = append(allBlockHashes.Hashes, hash)
	}
	return allBlockHashes, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
