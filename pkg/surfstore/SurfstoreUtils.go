package surfstore

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	baseDir := client.BaseDir
	fmt.Println("step1")
	// step1: fetch local file info
	localMetaMap := make(map[string][]string)
	localBlockMap := make(map[string][]*Block)
	localHashBlockMap := make(map[string]map[string]*Block)
	// fmt.Printf("%v\n", baseDir)
	err := filepath.Walk(baseDir, func(path string, info os.FileInfo, err error) error {
		// fmt.Printf("walk once: %v\n", info.Name())
		if err != nil {
			return err
		}
		if info.IsDir() {
			if path == baseDir {
				return nil
			} else {
				return fmt.Errorf("error containing directory")
			}
		}
		fileName := info.Name()
		if fileName == "index.db" {
			return nil
		}
		//file, err := os.Open(path)
		hashList := []string{}
		blockList := []*Block{}
		// if err != nil {
		// 	return err
		// }
		buf, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		totalBytesRead := len(buf)
		// bytesRead, err := ReadBlock(file, buf)
		// if bytesRead == 0 {
		// 	break
		// }
		bytesRead := 0
		for {
			if totalBytesRead-bytesRead > 4096 {
				blockData := buf[bytesRead : bytesRead+4096]
				hashList = append(hashList, GetBlockHashString(blockData))
				block := &Block{
					BlockData: blockData,
					BlockSize: int32(len(blockData)),
				}
				blockList = append(blockList, block)
				bytesRead += 4096
			} else {
				blockData := buf[bytesRead:]
				if len(blockData) == 0 {
					hashList = append(hashList, "-1")
				} else {
					hashList = append(hashList, GetBlockHashString(blockData))
				}
				block := &Block{
					BlockData: blockData,
					BlockSize: int32(len(blockData)),
				}
				blockList = append(blockList, block)
				break
			}
		}
		// if err == io.EOF || bytesRead == 0 {
		// 	break
		// }
		localMetaMap[fileName] = hashList
		localBlockMap[fileName] = blockList
		localHashBlockMap[fileName] = make(map[string]*Block)
		for i := range localMetaMap[fileName] {
			localHashBlockMap[fileName][hashList[i]] = blockList[i]
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Error fetching local file info: %v\n", err)
	}
	// fmt.Printf("%v\n", localMetaMap)
	//fmt.Printf("%v\n", localBlockMap)
	fmt.Println("step2")
	// step2: fetch local index.db map
	localIndexMap := make(map[string]*FileMetaData)
	if _, err := os.Stat(baseDir + "/" + "index.db"); os.IsNotExist(err) {
		// no index.db
	} else {
		// index.db exists
		localMap, err := LoadMetaFromMetaFile(baseDir)
		if err != nil {
			log.Fatalf("Error Reading index.db: %v\n", err)
		}
		for k, v := range localMap {
			localIndexMap[k] = v
		}
	}
	PrintMetaMap(localIndexMap)
	fmt.Println("step3")
	// step3: fetch and push all local changes to cloud
	blockStoreMap := make(map[string][]string)
	// check newly created or modified file
	for fileName, hashList := range localMetaMap {
		err := client.GetBlockStoreMap(hashList, &blockStoreMap)
		if err != nil {
			log.Fatalf("Error Getting block store map: %v\n", err)
		}
		if fileMetaData, ok := localIndexMap[fileName]; !ok {
			// new created local file
			fileMetaData := &FileMetaData{
				Filename:      fileName,
				Version:       1,
				BlockHashList: hashList,
			}
			// fmt.Printf("Create a new file to cloud\n")
			err = Push(&client, fileMetaData, localHashBlockMap[fileName], blockStoreMap)
			if err != nil {
				log.Fatalf("Error Creating new File on Cloud: %v\n", err)
			}
		} else {
			// both contain the file, then compare the hash list
			if !CompareHashLists(hashList, fileMetaData.GetBlockHashList()) {
				// if not equal, we need to push it to cloud
				newFileMetaData := &FileMetaData{
					Filename:      fileName,
					Version:       fileMetaData.Version + 1,
					BlockHashList: hashList,
				}
				err := Push(&client, newFileMetaData, localHashBlockMap[fileName], blockStoreMap)
				if err != nil {
					log.Fatalf("Error Writing File on Cloud: %v\n", err)
				}
			}
		}
	}
	fmt.Println("step3.5")
	// check deleted file
	for fileName, fileMetaData := range localIndexMap {
		if _, ok := localMetaMap[fileName]; !ok && fileMetaData.GetBlockHashList()[0] != "0" {
			// file in index.db, but not in directory
			newFileMetaData := &FileMetaData{
				Filename:      fileName,
				Version:       fileMetaData.Version + 1,
				BlockHashList: []string{"0"},
			}
			err := Push(&client, newFileMetaData, localHashBlockMap[fileName], make(map[string][]string))
			if err != nil {
				log.Fatalf("Error deleting File on cloud: %v\n", err)
			}

		}
	}

	fmt.Println("step4")
	// step4: fetch remote index.db map
	remoteIndexMap := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndexMap)
	if err != nil {
		log.Fatalf("Error Fetching Remote index.db: %v\n", err)
	}
	PrintMetaMap(remoteIndexMap)

	fmt.Println("step5")
	// step5: pull remote changes to local
	// check newly created or modified file on cloud
	for fileName, fileMetaData := range remoteIndexMap {
		err := client.GetBlockStoreMap(fileMetaData.GetBlockHashList(), &blockStoreMap)
		if err != nil {
			log.Fatalf("Error Getting block store map: %v\n", err)
		}
		if hashList, ok := localMetaMap[fileName]; !ok {
			// no such file on local
			// fmt.Printf("create new file on local\n")
			err := Pull(&client, fileMetaData, baseDir, blockStoreMap)
			if err != nil {
				log.Fatalf("Error Write local file: %v\n", err)
			}
		} else {
			if !CompareHashLists(hashList, fileMetaData.GetBlockHashList()) {
				// local version is out-dated
				err := Pull(&client, fileMetaData, baseDir, blockStoreMap)
				if err != nil {
					log.Fatalf("Error Update local file: %v\n", err)
				}
			}
		}
	}

	fmt.Println("step6")
	// step6: sync local index.db
	err = WriteMetaFile(remoteIndexMap, baseDir)
	if err != nil {
		log.Fatalf("Error Update local index.db: %v\n", err)
	}
}

func Pull(client *RPCClient, fileMetaData *FileMetaData, baseDir string, blockStoreMap map[string][]string) error {
	block := Block{}
	fileName := fileMetaData.GetFilename()
	// fmt.Printf("file path: %v\n", baseDir+"/"+fileName)
	file, err := os.OpenFile(baseDir+"/"+fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}
	_, err = file.Seek(0, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	for addr, hashList := range blockStoreMap {
		for _, hash := range hashList {
			if hash == "0" {
				// need to delete local file
				err := os.Remove(baseDir + "/" + fileName)
				if err != nil {
					return err
				}
				return nil
			}
			if hash == "-1" {
				return nil
			}
			err := client.GetBlock(hash, addr, &block)
			if err != nil {
				return err
			}
			//fmt.Printf("Block data: %v\n", block.GetBlockData())
			err = WriteBlock(file, block.GetBlockData(), int(block.GetBlockSize()))
			if err != nil {
				return err
			}

		}
	}

	return nil
}

func Push(client *RPCClient, fileMetaData *FileMetaData, hashBlockMap map[string]*Block, blockStoreMap map[string][]string) error {
	var version int32
	err := client.UpdateFile(fileMetaData, &version)
	if err != nil {
		return err
	}
	//
	// fmt.Printf("version: %v\n", int(version))
	if int(version) == -1 {
		return nil
	}
	var succ bool
	for addr, hashList := range blockStoreMap {
		for _, hash := range hashList {
			err := client.PutBlock(hashBlockMap[hash], addr, &succ)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func ReadBlock(file *os.File, buf []byte) (int, error) {
	totalBytesRead := 0
	clearBuffer(buf)
	// bytesRead, err := os.ReadFile(file)
	// for totalBytesRead < 4096 {
	// 	bytesRead, err := file.Read(buf[totalBytesRead:])
	// 	fmt.Printf("bytesread inseide: %v\n", bytesRead)
	// 	if err != nil {
	// 		return totalBytesRead, err
	// 	}
	// 	if bytesRead == 0 {
	// 		break
	// 	}
	// 	totalBytesRead += bytesRead
	// }
	return totalBytesRead, nil
}

func WriteBlock(file *os.File, buf []byte, sz int) error {
	// fmt.Printf("buf size: %v\n", len(buf))
	_, err := file.Write(buf)
	if err != nil {
		return err
	}
	return nil
	// totalWritten := 0
	// //fmt.Printf("wrietblock: %v\n", buf)
	// for totalWritten < sz {
	// 	n, err := file.Write(buf[totalWritten:])
	// 	if err != nil {
	// 		return err
	// 	}
	// 	fmt.Printf("totalwritten: %v\n", totalWritten)
	// 	totalWritten += n
	// }
	// return nil
}

func clearBuffer(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}

func CompareHashLists(hl1, hl2 []string) bool {
	if len(hl1) != len(hl2) {
		return false
	}
	for i := range hl1 {
		if hl1[i] != hl2[i] {
			return false
		}
	}
	return true
}
