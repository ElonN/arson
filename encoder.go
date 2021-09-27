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
	shard_header_size = 32
	type_data         = 0xf1
	type_parity       = 0xf2
)

var (
	arg_num_data_shards   = flag.Int("ds", 10, "Number of data shards")
	arg_num_parity_shards = flag.Int("ps", 3, "Number of parity shards")
	arg_full_shard_size   = flag.Int("mss", 1300, "Maximum segment size to send (shard + header)")
	arg_input_file        = flag.String("f", "", "Input file")
	arg_enc_out_dir       = flag.String("eout", "", "Encoder output directory")
	arg_dec_out_dir       = flag.String("dout", "", "Decoder output directory")
)

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
	chunk_data_size int
	chunk_idx       int
	file_id         int
	file_size       int
	total_chunks    int
	shards          [][]byte
	chunk_buffer    []byte
}

type FileStatus struct {
	file_id           uint64
	file_size         uint64
	total_chunks      int
	arrived_chunks    int
	finished_chunks   int
	num_data_shards   byte
	num_parity_shards byte
	num_total_shards  byte
	shard_data_size   int
}

type FECFileEncoder struct {
	num_data_shards    int
	num_parity_shards  int
	total_shards       int
	full_shard_size    int
	shard_data_size    int
	total_chunk_buffer int
	type_data          byte
	type_parity        byte
	max_chunk_size     int
	idGen              *snowflake.Node
}

func newFECFileEncoder(data_shards, parity_shards, full_shard_size int) *FECFileEncoder {
	enc := new(FECFileEncoder)
	enc.num_data_shards = data_shards
	enc.num_parity_shards = parity_shards
	enc.total_shards = data_shards + parity_shards
	enc.full_shard_size = full_shard_size
	enc.shard_data_size = full_shard_size - shard_header_size
	enc.total_chunk_buffer = enc.total_shards * enc.shard_data_size
	enc.type_data = 0xf1
	enc.type_parity = 0xf2
	enc.max_chunk_size = enc.shard_data_size * enc.num_data_shards
	enc.idGen, _ = snowflake.NewNode(1)
	return enc
}

type FECFileDecoder struct {
	num_data_shards    int
	num_parity_shards  int
	total_shards       int
	full_shard_size    int
	shard_data_size    int
	total_chunk_buffer int
	type_data          byte
	type_parity        byte
	max_chunk_size     int
	all_files          map[uint64][]Chunk
	all_files_sync     map[uint64]*FileStatus
	new_file_mutex     sync.Mutex
}

func newFECFileDecoder(data_shards, parity_shards, full_shard_size int) *FECFileDecoder {
	dec := new(FECFileDecoder)
	dec.num_data_shards = data_shards
	dec.num_parity_shards = parity_shards
	dec.total_shards = data_shards + parity_shards
	dec.full_shard_size = full_shard_size
	dec.shard_data_size = full_shard_size - shard_header_size
	dec.total_chunk_buffer = dec.total_shards * dec.shard_data_size
	dec.type_data = 0xf1
	dec.type_parity = 0xf2
	dec.max_chunk_size = dec.shard_data_size * dec.num_data_shards
	return dec
}

func parse_shard_header(b []byte) (file_id uint64, file_size uint64, chunk_ord uint32,
	total_chunks uint32, chunk_data_size uint32, shard_idx uint16, num_data_shards byte,
	num_parity_shards byte) {
	//
	//The header format:
	// |                      file_id(8B)                         |
	// |                      file_size(8B)                       |
	// |      chunk_ord (4B)          |       total_chunks(4B)    |
	// |	  chunk_data_size(4B)	  |shard_idx(2B)|DS(1B)|PS(1B)|
	//
	//
	file_id = binary.LittleEndian.Uint64(b)
	file_size = binary.LittleEndian.Uint64(b[8:])
	chunk_ord = binary.LittleEndian.Uint32(b[16:])
	total_chunks = binary.LittleEndian.Uint32(b[20:])
	chunk_data_size = binary.LittleEndian.Uint32(b[24:])
	shard_idx = binary.LittleEndian.Uint16(b[28:])
	num_data_shards = byte(b[30])
	num_parity_shards = byte(b[31])
	return
}

func (enc *FECFileEncoder) mark_shard_header(b []byte, c *Chunk, idx int) {
	//
	//The header format:
	// |                      file_id(8B)                         |
	// |                      file_size(8B)                       |
	// |      chunk_idx (4B)          |       total_chunks(4B)    |
	// |	  chunk_data_size(4B)	  |shard_idx(2B)|DS(1B)|PS(1B)|
	//
	//
	binary.LittleEndian.PutUint64(b, uint64(c.file_id))
	binary.LittleEndian.PutUint64(b[8:], uint64(c.file_size))
	binary.LittleEndian.PutUint32(b[16:], uint32(c.chunk_idx))
	binary.LittleEndian.PutUint32(b[20:], uint32(c.total_chunks))
	binary.LittleEndian.PutUint32(b[24:], uint32(c.chunk_data_size))
	binary.LittleEndian.PutUint16(b[28:], uint16(idx))
	b[30] = uint8(*arg_num_data_shards)
	b[31] = uint8(*arg_num_parity_shards)
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func (enc *FECFileEncoder) get_chunks(filename string, chunk_size int, file_id int) ([]Chunk, error) {
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
		chunks[i].chunk_data_size = chunk_size
		chunks[i].chunk_idx = i
		chunks[i].file_id = file_id
		chunks[i].total_chunks = num_chunks
		chunks[i].file_size = int(filesize)
		chunks[i].chunk_buffer = make([]byte, enc.total_shards*enc.shard_data_size)
	}
	// last one is the remainder
	chunks[num_chunks-1].chunk_data_size = int(filesize) - chunks[num_chunks-1].chunk_idx*chunk_size

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
			// chunk_buffer = data_from_file + zero_padding + space_for_parity_shards
			//				 |chunk_data_size|

			data_from_file := this_chunk.chunk_buffer[:this_chunk.chunk_data_size]
			bytes_read, err := file.ReadAt(data_from_file, int64(this_chunk.chunk_idx)*int64(chunk_size))

			log.Debug("Read ", bytes_read, " / ", this_chunk.chunk_data_size, " bytes for chunk ", i)

			if bytes_read != this_chunk.chunk_data_size {
				fatalErrors <- fmt.Errorf("get_chunks: chunk %d at ordinal %d read %d bytes (expected %d)",
					i, this_chunk.chunk_idx, bytes_read, this_chunk.chunk_data_size)
			}
			if err != nil {
				fatalErrors <- err
			}

			// "shards" are slices from chunk_buffer
			this_chunk.shards = make([][]byte, enc.total_shards)
			for j := 0; j < enc.total_shards; j++ {
				idx_start := enc.shard_data_size * j
				idx_end := idx_start + enc.shard_data_size
				this_chunk.shards[j] = this_chunk.chunk_buffer[idx_start:idx_end]
			}

			// Create encoding matrix.
			enc, err := reedsolomon.New(enc.num_data_shards, enc.num_parity_shards)
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

func (enc *FECFileEncoder) send_chunks(chunks []Chunk, out_base string) error {
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

			for j := 0; j < enc.total_shards; j++ {
				f, err := os.OpenFile(fmt.Sprintf("%s.%04d.%04d", out_base, i, j),
					os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					fatalErrors <- err
					return
				}
				var header [shard_header_size]byte
				enc.mark_shard_header(header[:], &c, j)
				bytes_written, err := f.Write(header[:])
				if err != nil {
					fatalErrors <- err
				}
				if bytes_written != shard_header_size {
					fatalErrors <- fmt.Errorf("get_chunks: shard %d at chunk %d read %d bytes (expected %d)",
						j, i, bytes_written, shard_header_size)
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

func (enc *FECFileEncoder) encode(filename string, out_dir string) {

	chunks, err := enc.get_chunks(filename, enc.max_chunk_size, int(enc.idGen.Generate()))
	checkErr(err)

	err = enc.send_chunks(chunks, filepath.Join(out_dir, filepath.Base(filename)))
	checkErr(err)

	log.Debug("Finished")
}

func (dec *FECFileDecoder) read_shard(shard []byte) {
	// read header from beginning
	file_id, file_size, chunk_ord, total_chunks, chunk_data_size,
		shard_idx, num_data_shards, num_parity_shards := parse_shard_header(shard[:shard_header_size])
	log.Debug("shard header is ", file_id, file_size, chunk_ord, total_chunks, chunk_data_size,
		shard_idx, num_data_shards, num_parity_shards)

	// lock when checking if new file arrived
	dec.new_file_mutex.Lock()
	if _, ok := dec.all_files[file_id]; !ok {
		// NEW FILE - initialize chunk array and sync object
		dec.all_files[file_id] = make([]Chunk, total_chunks)
		dec.all_files_sync[file_id] = new(FileStatus)

		file_chunks := dec.all_files[file_id]
		stat := dec.all_files_sync[file_id]
		stat.num_data_shards = num_data_shards
		stat.num_parity_shards = num_parity_shards
		stat.num_total_shards = num_data_shards + num_parity_shards
		stat.file_size = file_size
		stat.file_id = file_id
		stat.total_chunks = int(total_chunks)
		stat.shard_data_size = len(shard) - shard_header_size

		// initialize all chunks for this file
		for j := 0; j < stat.total_chunks; j++ {
			// chunk_buffer holds the actual data of the chunk (without headers)
			// NOTE: chunk_buffer is padded with zeros, actual size is in chunk_data_size
			file_chunks[j].chunk_buffer = make([]byte,
				int(stat.num_total_shards)*stat.shard_data_size)
			file_chunks[j].chunk_idx = j
			file_chunks[j].shards = make([][]byte, int(stat.num_total_shards))
			// initialize all shards for this chunk
			// each shard points to its slice in the chunk_buffer
			for k := 0; k < int(stat.num_total_shards); k++ {
				// Default is slice with zero length
				// This is important for reconstruction, see docs for "reedsolomon.Reconstruct"
				idx_start := stat.shard_data_size * k
				file_chunks[j].shards[k] = file_chunks[j].chunk_buffer[idx_start:idx_start]
			}
		}
	}
	dec.new_file_mutex.Unlock()
	// fill in chunk data
	this_chunk := &dec.all_files[file_id][chunk_ord]
	this_chunk.chunk_data_size = int(chunk_data_size)
	// copy shard data to chuckBuf
	idx_start := int(shard_idx) * dec.all_files_sync[file_id].shard_data_size
	idx_end := idx_start + dec.all_files_sync[file_id].shard_data_size
	copy(this_chunk.chunk_buffer[idx_start:idx_end], shard[shard_header_size:])

	// make shard slice point to where the data is in chunk_buffer
	this_chunk.shards[shard_idx] = this_chunk.chunk_buffer[idx_start:idx_end]

}

func (dec *FECFileDecoder) read_chunks(foldername string) {
	files, err := ioutil.ReadDir(foldername)
	checkErr(err)

	for _, f := range files {

		filename := filepath.Join(foldername, f.Name())

		log.Debug("Opening ", filename)
		shard, err := ioutil.ReadFile(filename)
		checkErr(err)

		dec.read_shard(shard)
	}
}

func (dec *FECFileDecoder) reconstruct_chunks(file_id uint64) {
	// Create matrix
	chunks := dec.all_files[file_id]
	file_status := dec.all_files_sync[file_id]
	enc, err := reedsolomon.New(int(file_status.num_data_shards), int(file_status.num_parity_shards))
	checkErr(err)
	for _, chunk := range chunks {
		ok, err := enc.Verify(chunk.shards)
		if ok {
			log.Debugf("Verified chunk no. %d / %d", chunk.chunk_idx, file_status.total_chunks-1)
		} else {
			log.Debugf("Verification failed  chunk no. %d / %d. Reconstructing data",
				chunk.chunk_idx, file_status.total_chunks-1)
			err = enc.Reconstruct(chunk.shards)
			if err != nil {
				checkErr(err)
			}
			ok, err = enc.Verify(chunk.shards)
			if !ok {
				log.Debugf("Reconstruction failed  chunk no. %d / %d. Reconstructing data",
					chunk.chunk_idx, file_status.total_chunks-1)
				checkErr(err)
			}
			log.Debugf("Reconstructed! Verified chunk no. %d / %d", chunk.chunk_idx, file_status.total_chunks-1)

		}
		checkErr(err)
	}
}

func (dec *FECFileDecoder) recover_files(out_dir string) {

	for id, file_chunks := range dec.all_files {
		outfile_basename := fmt.Sprintf("%d.out", id)
		outfile_fullname := filepath.Join(out_dir, outfile_basename)
		f, err := os.OpenFile(outfile_fullname, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
		checkErr(err)
		defer f.Close()
		for _, chunk := range file_chunks {
			f.Write(chunk.chunk_buffer[:chunk.chunk_data_size])
		}
		log.Debugf("Wrote file to %s", outfile_fullname)
	}
}

func (dec *FECFileDecoder) decode(input_folder string, output_dir string) {
	dec.all_files = make(map[uint64][]Chunk)
	dec.all_files_sync = make(map[uint64]*FileStatus)
	dec.read_chunks(input_folder)

	for k, _ := range dec.all_files {
		log.Debug("starting reconstruct for file ", k)
		dec.reconstruct_chunks(k)
	}

	dec.recover_files(output_dir)

}

func main() {
	// Parse command line parameters.
	flag.Parse()

	encoder := newFECFileEncoder(*arg_num_data_shards, *arg_num_parity_shards, *arg_full_shard_size)
	decoder := newFECFileDecoder(*arg_num_data_shards, *arg_num_parity_shards, *arg_full_shard_size)

	log.Debug("before encode")
	encoder.encode(*arg_input_file, *arg_enc_out_dir)
	log.Debug("before decode")
	decoder.decode(*arg_enc_out_dir, *arg_dec_out_dir)
	log.Debug("finish")
}

func checkErr(err error) {
	if err != nil {
		log.Error(err)
		os.Exit(2)
	}
}
