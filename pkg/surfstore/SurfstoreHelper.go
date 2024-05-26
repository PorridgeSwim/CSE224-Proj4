package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = `INSERT INTO indexes (fileName, version, hashIndex, hashValue) VALUES (?, ?, ?, ?);`

//const testTuple string = `SELECT * FROM indexes`

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	// fmt.Printf("write meta file path: %v\n", outputMetaPath)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}

	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}
	statement.Exec()

	insertStatement, err := db.Prepare(insertTuple)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
	}

	for fileName, metaData := range fileMetas {
		hashList := metaData.GetBlockHashList()
		for index, value := range hashList {
			_, err := insertStatement.Exec(fileName, metaData.GetVersion(), index, value)
			if err != nil {
				log.Fatal("Error During Meta Write Back")
			}
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = `SELECT DISTINCT fileName FROM indexes;`

const getTuplesByFileName string = `SELECT * FROM indexes WHERE fileName = ? ORDER BY hashIndex`

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}

	rows, err := db.Query(getDistinctFileName)
	if err != nil {
		log.Fatal("Error When Getting Distinct FileName")
	}

	for rows.Next() {
		var fileName string
		err := rows.Scan(&fileName)
		if err != nil {
			log.Fatal("Error Scanning FileName")
		}

		tuples, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			log.Fatal("Error Getting Tuples By FileName")
		}
		hashList := []string{}
		var version int32
		for tuples.Next() {
			var index int
			var value string
			err := tuples.Scan(&fileName, &version, &index, &value)
			if err != nil {
				log.Fatal("Error Scanning Tupples")
			}
			hashList = append(hashList, value)
		}

		fileMetaMap[fileName] = &FileMetaData{
			Filename:      fileName,
			Version:       version,
			BlockHashList: hashList,
		}
	}

	return fileMetaMap, nil

}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
