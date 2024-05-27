package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap map[string]*FileMetaData
	// BlockStoreAddr string
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{
		FileInfoMap: m.FileMetaMap,
	}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fileName := fileMetaData.GetFilename()
	if prevMetaData, ok := m.FileMetaMap[fileName]; ok {
		prevVersion := prevMetaData.GetVersion()
		currVersion := fileMetaData.GetVersion()
		if prevVersion+1 == currVersion {
			m.FileMetaMap[fileName] = fileMetaData
			return &Version{Version: currVersion}, nil
		} else {
			return &Version{Version: -1}, nil
		}
	} else {
		m.FileMetaMap[fileName] = fileMetaData
		if fileMetaData.GetVersion() != int32(1) {
			return &Version{Version: -1}, nil
		}
		return &Version{Version: fileMetaData.GetVersion()}, nil
	}
}

/*
func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
}*/

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	//fmt.Println("begin metastore get block store map")
	blockStoreMap := &BlockStoreMap{BlockStoreMap: make(map[string]*BlockHashes)}
	for _, addr := range m.BlockStoreAddrs {
		blockStoreMap.BlockStoreMap[addr] = &BlockHashes{Hashes: []string{}}
	}
	for _, hash := range blockHashesIn.GetHashes() {
		serverAddr := m.ConsistentHashRing.GetResponsibleServer(hash)
		blockStoreMap.BlockStoreMap[serverAddr].Hashes = append(blockStoreMap.BlockStoreMap[serverAddr].Hashes, hash)
	}
	// for _, addr := range m.BlockStoreAddrs {
	// 	sort.Strings(blockStoreMap.BlockStoreMap[addr].Hashes)
	// }
	//fmt.Println("firnish metastore get block store map")
	return blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

/*
func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}*/

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
