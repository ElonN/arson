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
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/reedsolomon"
	log "github.com/sirupsen/logrus"
)

const (
	dataShards       = 10
	parShards        = 3
	totalShards      = dataShards + parShards
	shardHeaderSize  = 26
	shardSize        = 1300
	maxShardDataSize = shardSize - shardHeaderSize
	totalChunkBuffer = totalShards * shardSize
	typeData         = 0xf1
	typeParity       = 0xf2
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	// log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
}

var inputFile = flag.String("f", "", "Input file")
var outDir = flag.String("out", "", "Alternative output directory")

var maxChunkSize = (int64(shardSize) - shardHeaderSize) * int64(dataShards)

type chunk struct {
	bufsize         int64
	offset          int64
	numDataShards   uint8
	numParityShards uint8
	file_id         uint64
	bytesUsed       uint16
	totalFileSize   uint64
	shards          [][]byte
	chunkBuffer     [totalChunkBuffer]byte
}

func get_shards(c chunk) {
	//
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func get_chunks(filename string, chunk_size int64) ([]chunk, error) {
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
	concurrency := int(math.Ceil(float64(filesize) / float64(chunk_size)))
	log.Debug("Concurrency is ", concurrency)
	chunks := make([]chunk, concurrency)

	for i := 0; i < concurrency; i++ {
		chunks[i].bufsize = chunk_size
		chunks[i].offset = chunk_size * int64(i)
	}
	// last one is the remainder
	chunks[concurrency-1].bufsize = filesize - chunks[concurrency-1].offset

	// Make channels to pass fatal errors in WaitGroup
	fatalErrors := make(chan error)
	wgDone := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(concurrency)

	log.Debug("Starting chunk goroutines")
	for i := 0; i < concurrency; i++ {
		// Each of these go routines will fill shards for a chunk
		go func(chunksizes []chunk, i int) {
			defer wg.Done()
			log.Debug("Inside chunk goroutine number ", i)
			this_chunk := &chunksizes[i]

			//				 |------DATA SHARDS-----------| |----PARITY SHARDS-----|
			// chunkBuffer = data_from_file + zero_padding + space_for_parity_shards
			//				 |---bufsize---|

			data_from_file := this_chunk.chunkBuffer[:this_chunk.bufsize]
			bytesRead, err := file.ReadAt(data_from_file, this_chunk.offset)

			log.Debug("Read ", bytesRead, " / ", this_chunk.bufsize, " bytes for chunk ", i)

			if int64(bytesRead) != this_chunk.bufsize {
				fatalErrors <- fmt.Errorf("get_chunks: chunk %d at offset %d read %d bytes (expected %d)",
					i, this_chunk.offset, bytesRead, this_chunk.bufsize)
			}
			if err != nil {
				fatalErrors <- err
			}

			// "shards" are slices from chunkBuffer
			this_chunk.shards = make([][]byte, totalShards)
			for j := 0; j < totalShards; j++ {
				idx_start := shardSize * j
				idx_end := shardSize * (j + 1)
				this_chunk.shards[j] = this_chunk.chunkBuffer[idx_start:idx_end]
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

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  simple-encoder [-flags] filename.ext\n\n")
		fmt.Fprintf(os.Stderr, "Valid flags:\n")
		flag.PrintDefaults()
	}
}

func (c *chunk) getId(filename string) {

}

func main() {
	// Parse command line parameters.
	flag.Parse()

	if *inputFile == "" {
		fmt.Fprintf(os.Stderr, "Error: No input filename given\n")
		flag.Usage()
		os.Exit(1)
	}
	log.Debug("OK, file is ", *inputFile)

	chunks, err := get_chunks(*inputFile, maxChunkSize)
	checkErr(err)

	out_base := filepath.Join(*outDir, filepath.Base(*inputFile))
	log.Debugf("OUTPUT is in: %s.%04d", out_base, 1)

	// Make channels to pass fatal errors in WaitGroup
	fatalErrors := make(chan error)
	wgDone := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(len(chunks))

	log.Debug("Starting encode goroutines")
	for i, c := range chunks {
		go func(i int, c chunk) {
			log.Debug("Inside encode goroutine number ", i)
			// Create encoding matrix.
			enc, err := reedsolomon.New(dataShards, parShards)
			if err != nil {
				fatalErrors <- err
			}

			err = enc.Encode(c.shards)
			if err != nil {
				fatalErrors <- err
			}

			for j := 0; j < totalShards; j++ {
				f, err := os.OpenFile(fmt.Sprintf("%s.%04d.%04d", out_base, i, j),
					os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					fatalErrors <- err
					return
				}
				f.WriteString(fmt.Sprintf("|---HEADER of %04d shard of %04d chunk---|\n", j, i))
				if err != nil {
					fatalErrors <- err
				}

				f.Write(c.shards[j])
				if err != nil {
					fatalErrors <- err
				}
				f.Close()
			}

			log.Debug("Finished encode goroutine number ", i)
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
		checkErr(err)
	}
	log.Debug("Finished")
}

func checkErr(err error) {
	if err != nil {
		log.Error(err)
		os.Exit(2)
	}
}
