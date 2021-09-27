// Copyright 2015, Klaus Post, see LICENSE for details.
//
// Simple encoder example
//
// The encoder encodes a simgle file into a number of shards
// To reverse the process see "simpledecoder.go"
//
// To build an executable use:
//
// go build simple-decoder.go
//
// Simple Encoder/Decoder Shortcomings:
// * If the file size of the input isn't divisible by the number of data shards
//   the output will contain extra zeroes
//
// * If the shard numbers isn't the same for the decoder as in the
//   encoder, invalid output will be generated.
//
// * If values have changed in a shard, it cannot be reconstructed.
//
// * If two shards have been swapped, reconstruction will always fail.
//   You need to supply the shards in the same order as they were given to you.
//
// The solution for this is to save a metadata file containing:
//
// * File size.
// * The number of data/parity shards.
// * HASH of each shard.
// * Order of the shards.
//
// If you save these properties, you should abe able to detect file corruption
// in a shard and be able to reconstruct your data if you have the needed number of shards left.

package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/bwmarrin/snowflake"
	"github.com/klauspost/reedsolomon"
	log "github.com/sirupsen/logrus"
)

const (
	numDataShards    = 10
	numParShards     = 3
	totalShards      = numDataShards + numParShards
	shardHeaderSize  = 24
	fullShardSize    = 1300
	shardDataSize    = fullShardSize - shardHeaderSize
	totalChunkBuffer = totalShards * shardDataSize
	typeData         = 0xf1
	typeParity       = 0xf2
)

var inputFile = flag.String("f", "", "Input file")
var outDir = flag.String("out", "", "Alternative output directory")
var maxChunkSize = shardDataSize * numDataShards
var all_files map[uint64][]Chunk
var all_files_sync map[uint64][]sync.WaitGroup
var new_file_mutex sync.Mutex
var debug_file_id uint64

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  simple-encoder [-flags] filename.ext\n\n")
		fmt.Fprintf(os.Stderr, "Valid flags:\n")
		flag.PrintDefaults()
	}
	// Log as JSON instead of the default ASCII formatter.
	// log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
}

type Chunk struct {
	bufsize      int
	chunk_ord    int
	file_id      int
	total_chunks int
	shards       [][]byte
	chunkBuffer  [totalChunkBuffer]byte
}

func parse_shard_header(b []byte) (uint64, uint32, uint32, uint16, uint16, byte, byte) {

	//
	//The header format:
	// |                      file_id(8B)                          |
	// |      chunk_ord (4B)           |       total_chunks(4B)    |
	// |shard_ord(2B)|shardDataSize(2B)|DS(1B)|PS(1B)| RESERVED(2B)|
	//
	//
	return binary.LittleEndian.Uint64(b),
		binary.LittleEndian.Uint32(b[8:]),
		binary.LittleEndian.Uint32(b[12:]),
		binary.LittleEndian.Uint16(b[16:]),
		binary.LittleEndian.Uint16(b[18:]),
		byte(b[20]),
		byte(b[21])
}

func mark_shard_header(b []byte, c *Chunk, idx int) {

	//
	//The header format:
	// |                      file_id(8B)                          |
	// |      chunk_ord (4B)           |       total_chunks(4B)    |
	// |shard_ord(2B)|shardDataSize(2B)|DS(1B)|PS(1B)| RESERVED(2B)|
	//
	//
	binary.LittleEndian.PutUint64(b, uint64(c.file_id))
	binary.LittleEndian.PutUint32(b[8:], uint32(c.chunk_ord))
	binary.LittleEndian.PutUint32(b[12:], uint32(c.total_chunks))
	binary.LittleEndian.PutUint16(b[16:], uint16(idx))
	//binary.LittleEndian.PutUint16(b[18:], uint16(shardSize))
	//if c.chunk_ord == c.total_chunks-1 { // LAST CHUNK
	// File data ends at some shard in the last chunk
	// It looks something like this: (|-s-| is one data shard)
	//
	// |-s-|-s-|-s-| ... |-s-|-s-|-s-|-s-|-s-|-s-|-s-|
	// |---file data-----------|--zeros--|---parity--|
	//
	// The first shards are full
	// The last shards are zeros
	// One special shard is partial
	full_shards := c.bufsize / shardDataSize
	remainder := c.bufsize % shardDataSize
	var shard_data_size int
	if idx > full_shards { // Empty shard
		shard_data_size = 0
	} else if idx == full_shards { // Partial shard
		shard_data_size = remainder
	} else { // Full shard
		shard_data_size = shardDataSize
	}
	binary.LittleEndian.PutUint16(b[18:], uint16(shard_data_size))
	//}
	b[20] = uint8(numDataShards)
	b[21] = uint8(numParShards)
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func get_chunks(filename string, chunk_size int, file_id int) ([]Chunk, error) {
	log.Debug("Opening ", filename)
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	filesize := fileinfo.Size()
	log.Debug("File size of ", filename, " is ", filesize)
	log.Debug("Chunk size is ", chunk_size)
	// Number of go routines we need to spawn.
	num_chunks := int(math.Ceil(float64(filesize) / float64(chunk_size)))
	log.Debug("num_chunks is ", num_chunks)
	chunks := make([]Chunk, num_chunks)

	for i := 0; i < num_chunks; i++ {
		chunks[i].bufsize = chunk_size
		chunks[i].chunk_ord = i
		chunks[i].file_id = file_id
		chunks[i].total_chunks = num_chunks
	}
	// last one is the remainder
	chunks[num_chunks-1].bufsize = int(filesize) - chunks[num_chunks-1].chunk_ord*chunk_size

	// Make channels to pass fatal errors in WaitGroup
	fatalErrors := make(chan error)
	wgDone := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(num_chunks)

	log.Debug("Starting chunk goroutines")
	for i := 0; i < num_chunks; i++ {
		// Each of these go routines will fill shards for a chunk
		go func(chunksizes []Chunk, i int) {
			defer wg.Done()
			log.Debug("Inside chunk goroutine number ", i)
			this_chunk := &chunksizes[i]

			//				 |------DATA SHARDS-----------| |----PARITY SHARDS-----|
			// chunkBuffer = data_from_file + zero_padding + space_for_parity_shards
			//				 |---bufsize---|

			data_from_file := this_chunk.chunkBuffer[:this_chunk.bufsize]
			bytesRead, err := file.ReadAt(data_from_file, int64(this_chunk.chunk_ord)*int64(chunk_size))

			log.Debug("Read ", bytesRead, " / ", this_chunk.bufsize, " bytes for chunk ", i)

			if bytesRead != this_chunk.bufsize {
				fatalErrors <- fmt.Errorf("get_chunks: chunk %d at ordinal %d read %d bytes (expected %d)",
					i, this_chunk.chunk_ord, bytesRead, this_chunk.bufsize)
			}
			if err != nil {
				fatalErrors <- err
			}

			// "shards" are slices from chunkBuffer
			this_chunk.shards = make([][]byte, totalShards)
			for j := 0; j < totalShards; j++ {
				idx_start := shardDataSize * j
				idx_end := shardDataSize * (j + 1)
				this_chunk.shards[j] = this_chunk.chunkBuffer[idx_start:idx_end]
			}

			// Create encoding matrix.
			enc, err := reedsolomon.New(numDataShards, numParShards)
			if err != nil {
				fatalErrors <- err
			}

			err = enc.Encode(this_chunk.shards)
			if err != nil {
				fatalErrors <- err
			}
		}(chunks, i)
	}

	// Important final goroutine to wait until WaitGroup is done
	go func() {
		wg.Wait()
		close(wgDone)
	}()

	// Wait until either WaitGroup is done or an error is received through the channel
	select {
	case <-wgDone:
		// carry on
		break
	case err := <-fatalErrors:
		wg.Wait()
		return nil, err
	}
	return chunks, nil
}

func send_chunks(chunks []Chunk) error {
	out_base := filepath.Join(*outDir, filepath.Base(*inputFile))
	log.Debugf("OUTPUT is in: %s.%04d", out_base, 1)

	// Make channels to pass fatal errors in WaitGroup
	fatalErrors := make(chan error)
	wgDone := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(len(chunks))

	log.Debug("Starting sending goroutines")
	for i, c := range chunks {
		go func(i int, c Chunk) {
			log.Debug("Inside sending goroutine number ", i)

			for j := 0; j < totalShards; j++ {
				f, err := os.OpenFile(fmt.Sprintf("%s.%04d.%04d", out_base, i, j),
					os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					fatalErrors <- err
					return
				}
				var header [shardHeaderSize]byte
				mark_shard_header(header[:], &c, j)
				bytesWritten, err := f.Write(header[:])
				if err != nil {
					fatalErrors <- err
				}
				if bytesWritten != shardHeaderSize {
					fatalErrors <- fmt.Errorf("get_chunks: shard %d at chunk %d read %d bytes (expected %d)",
						j, i, bytesWritten, shardHeaderSize)
				}

				f.Write(c.shards[j])
				if err != nil {
					fatalErrors <- err
				}
				f.Close()
			}

			log.Debug("Finished sending goroutine number ", i)
			wg.Done()
		}(i, c)
	}
	// Important final goroutine to wait until WaitGroup is done
	go func() {
		wg.Wait()
		close(wgDone)
	}()

	// Wait until either WaitGroup is done or an error is received through the channel
	select {
	case <-wgDone:
		// carry on
		break
	case err := <-fatalErrors:
		wg.Wait()
		return err
	}
	log.Debug("Finished send_chunks")
	return nil
}

func enc() {
	if *inputFile == "" {
		fmt.Fprintf(os.Stderr, "Error: No input filename given\n")
		flag.Usage()
		os.Exit(1)
	}
	log.Debug("OK, file is ", *inputFile)

	idGen, err := snowflake.NewNode(1)
	checkErr(err)

	chunks, err := get_chunks(*inputFile, maxChunkSize, int(idGen.Generate()))
	checkErr(err)

	err = send_chunks(chunks)
	checkErr(err)

	log.Debug("Finished")
}

func read_chunks(foldername string) {
	files, err := ioutil.ReadDir(foldername)
	checkErr(err)

	for _, f := range files {
		//go func(fi fs.FileInfo) {
		filename := filepath.Join(foldername, f.Name())
		log.Debug("Opening ", filename)
		file, err := os.Open(filename)
		checkErr(err)
		defer file.Close()

		var header [shardHeaderSize]byte
		numRead, err := file.Read(header[:])
		checkErr(err)
		if numRead < shardHeaderSize {
			checkErr(fmt.Errorf("read_chunks: file %s read %d bytes (expected %d)",
				filename, numRead, shardHeaderSize))
		}
		file_id, chunk_ord, total_chunks, shard_ord, shard_data_size, ds, ps := parse_shard_header(header[:])
		log.Debug("file ", filename, " has ", file_id, chunk_ord, total_chunks, shard_ord, shard_data_size, ds, ps)
		debug_file_id = file_id
		// lock when adding new file
		new_file_mutex.Lock()
		if _, ok := all_files[file_id]; !ok {
			all_files[file_id] = make([]Chunk, total_chunks)
			all_files_sync[file_id] = make([]sync.WaitGroup, total_chunks)
			for j, _ := range all_files_sync[file_id] {
				all_files_sync[file_id][j].Add(totalShards)
			}
			//do something here
		}
		new_file_mutex.Unlock()

		this_chunk := &all_files[file_id][chunk_ord]
		this_chunk.bufsize += int(shard_data_size)
		idx_start := shard_ord * shardDataSize
		idx_end := (shard_ord + 1) * shardDataSize
		numRead, err = file.Read(this_chunk.chunkBuffer[idx_start:idx_end])
		checkErr(err)
		if numRead < int(shardDataSize) {
			checkErr(fmt.Errorf("read_chunks: file %s ,file_id %d, shard %d read %d bytes (expected %d)",
				filename, file_id, shard_ord, numRead, shardDataSize))
		}
		all_files_sync[file_id][chunk_ord].Done()

		//}(f)
	}
	ddd := all_files[debug_file_id]
	_ = ddd
}

func recover_files() {
	nmap := all_files[debug_file_id]
	f, err := os.OpenFile("C:\\Elon\\temp\\nmap.out", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	checkErr(err)
	for _, c := range nmap {
		f.Write(c.chunkBuffer[:c.bufsize])
	}
	f.Close()
}

func dec() {
	all_files = make(map[uint64][]Chunk)
	all_files_sync = make(map[uint64][]sync.WaitGroup)
	read_chunks(*outDir)

	//reconstruct_chunks()

	recover_files()

}

func main() {
	// Parse command line parameters.
	flag.Parse()

	//enc()
	dec()
}

func checkErr(err error) {
	if err != nil {
		log.Error(err)
		os.Exit(2)
	}
}
