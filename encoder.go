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
	"time"

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
	arg_chunk_timeout     = flag.Int("to", 60, "Timeout for receiving chunks (in seconds)")
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
	chunk_data_size     int
	chunk_idx           int
	file_id             int
	file_size           int
	total_chunks        int
	shards              [][]byte
	chunk_buffer        []byte
	is_finished         bool
	arrived_shards      int
	arrived_data_shards int
	timestamp           int64
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
	is_finished       bool
	is_failed         bool
	timestamp         int64
	chunks            []Chunk
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
	all_files     map[uint64]*FileStatus
	chunk_timeout int64
}

func newFECFileDecoder(chunk_timeout int64) *FECFileDecoder {
	dec := new(FECFileDecoder)
	dec.chunk_timeout = chunk_timeout
	return dec
}

func parse_shard_header(b []byte) (file_id uint64, file_size uint64, chunk_idx uint32,
	total_chunks uint32, chunk_data_size uint32, shard_idx uint16, num_data_shards byte,
	num_parity_shards byte) {
	//
	//The header format:
	// |                      file_id(8B)                         |
	// |                      file_size(8B)                       |
	// |      chunk_idx (4B)          |       total_chunks(4B)    |
	// |	  chunk_data_size(4B)	  |shard_idx(2B)|DS(1B)|PS(1B)|
	//
	//
	file_id = binary.LittleEndian.Uint64(b)
	file_size = binary.LittleEndian.Uint64(b[8:])
	chunk_idx = binary.LittleEndian.Uint32(b[16:])
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
				fatalErrors <- fmt.Errorf("get_chunks: chunk %d at index %d read %d bytes (expected %d)",
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

func (fs *FileStatus) free_file() {

	for i := range fs.chunks {
		fs.chunks[i].shards = nil
		fs.chunks[i].chunk_buffer = nil
	}

}

func (dec *FECFileDecoder) put_shard(shard []byte, output_folder string) {
	// read header from beginning
	file_id, file_size, chunk_idx, total_chunks, chunk_data_size,
		shard_idx, num_data_shards, num_parity_shards := parse_shard_header(shard[:shard_header_size])
	log.Debug("shard header is ", file_id, file_size, chunk_idx, total_chunks, chunk_data_size,
		shard_idx, num_data_shards, num_parity_shards)

	// check if new file
	if _, ok := dec.all_files[file_id]; !ok {
		// NEW FILE - initialize chunk array and sync object
		dec.all_files[file_id] = new(FileStatus)
		this_file := dec.all_files[file_id]
		this_file.chunks = make([]Chunk, total_chunks)

		file_chunks := this_file.chunks
		this_file.num_data_shards = num_data_shards
		this_file.num_parity_shards = num_parity_shards
		this_file.num_total_shards = num_data_shards + num_parity_shards
		this_file.file_size = file_size
		this_file.file_id = file_id
		this_file.total_chunks = int(total_chunks)
		this_file.shard_data_size = len(shard) - shard_header_size

		// initialize all chunks for this file
		for j := 0; j < this_file.total_chunks; j++ {
			// chunk_buffer holds the actual data of the chunk (without headers)
			// NOTE: chunk_buffer is padded with zeros, actual size is in chunk_data_size
			file_chunks[j].chunk_buffer = make([]byte,
				int(this_file.num_total_shards)*this_file.shard_data_size)
			file_chunks[j].chunk_idx = j
			file_chunks[j].shards = make([][]byte, int(this_file.num_total_shards))
			// initialize all shards for this chunk
			// each shard points to its slice in the chunk_buffer
			for k := 0; k < int(this_file.num_total_shards); k++ {
				// Default is slice with zero length
				// This is important for reconstruction, see docs for "reedsolomon.Reconstruct"
				idx_start := this_file.shard_data_size * k
				file_chunks[j].shards[k] = file_chunks[j].chunk_buffer[idx_start:idx_start]
			}
		}

		// mark timestamp for file arrival start
		this_file.timestamp = time.Now().Unix()
	}

	this_file := dec.all_files[file_id]
	if this_file.is_finished {
		log.Warn("shard arrived for already finished file")
		return
	}
	if this_file.is_failed {
		log.Warn("shard arrived for already failed file")
		return
	}

	// now we know that the file exists, we will handle the specific shard
	// that has arrived. first, let's fill in chunk data.
	this_chunk := &this_file.chunks[chunk_idx]
	if this_chunk.is_finished {
		log.Warn("shard arrived for already finished chunk")
		return
	}
	if this_chunk.timestamp == 0 {
		this_chunk.timestamp = time.Now().Unix()
	}
	if this_chunk.chunk_data_size == 0 {
		this_chunk.chunk_data_size = int(chunk_data_size)
	}
	// if now is after timeout for chunk
	if time.Now().Unix() > this_chunk.timestamp+dec.chunk_timeout {
		log.Warnf("shard arrived for timed out chunk %d of file %d", chunk_idx, file_id)
		this_file.is_failed = true
		this_file.free_file()
		return
	}

	// copy shard data to chunk_buffer
	idx_start := int(shard_idx) * dec.all_files[file_id].shard_data_size
	idx_end := idx_start + dec.all_files[file_id].shard_data_size
	copy(this_chunk.chunk_buffer[idx_start:idx_end], shard[shard_header_size:])

	// make shard slice point to where the data is in chunk_buffer
	this_chunk.shards[shard_idx] = this_chunk.chunk_buffer[idx_start:idx_end]

	// keep track how many shards have arrived
	this_chunk.arrived_shards++
	if shard_idx < uint16(num_data_shards) {
		this_chunk.arrived_data_shards++
	}

	// check if we can reconstruct
	if this_chunk.arrived_shards >= int(num_data_shards) {
		// case 1 - all data shards arrived
		if this_chunk.arrived_data_shards == int(num_data_shards) {
			log.Debugf("all data shards arrived for file: %d chunk: %d",
				file_id, chunk_idx)
		} else {
			// case 2 - not all data shards have arrived,
			//          but they can be reconstructed
			log.Debugf("not all data shards arrived for file: %d chunk: %d, arrived %d / %d data shards",
				file_id, chunk_idx, this_chunk.arrived_data_shards, num_data_shards)
			rs, err := reedsolomon.New(int(this_file.num_data_shards),
				int(this_file.num_parity_shards))
			if err != nil {
				log.Error("reed solomon cannot be contructed: ", err)
				return
			}

			err = rs.Reconstruct(this_chunk.shards)
			if err != nil {
				log.Error("reconstruction failed: ", err)
				return
			}

		}
		log.Debugf("Finished file %d chunk %d, writing to output dir: %s",
			file_id, chunk_idx, output_folder)
		this_file.write_chunk(int(chunk_idx), output_folder)
		this_chunk.is_finished = true
	}

}

func (dec *FECFileDecoder) read_chunks(input_folder, output_folder string) {
	files, err := ioutil.ReadDir(input_folder)
	checkErr(err)

	for _, f := range files {

		filename := filepath.Join(input_folder, f.Name())

		log.Debug("Opening ", filename)
		shard, err := ioutil.ReadFile(filename)
		checkErr(err)

		dec.put_shard(shard, output_folder)
	}
}

func (fs *FileStatus) write_chunk(chunk_idx int, out_dir string) error {
	outfile_basename := fmt.Sprintf("%d.out", fs.file_id)
	outfile_fullname := filepath.Join(out_dir, outfile_basename)
	chunk := fs.chunks[chunk_idx]
	offset := chunk_idx * chunk.chunk_data_size
	if chunk_idx == fs.total_chunks-1 {
		// special case
		// if this is the last chunk, then chunk_data_size is remainder (file_size % max_chunk_size)
		// this means the offset cannot be calculated as above (chunk_idx * chunk_data_size)
		// luckily, for last chunk we can simply subtract chunk_data_size from total file size
		// this works even if the file is divided perfectly to equal size chunks (no remainder)
		offset = int(fs.file_size) - chunk.chunk_data_size
	}
	f, err := os.OpenFile(outfile_fullname, os.O_CREATE|os.O_WRONLY, 0644)
	checkErr(err)
	defer f.Close()
	log.Debugf("chunk WriteAt: chunk idx: %d,chunk_data_size: %d, offset: %d, filesize %d",
		chunk_idx, chunk.chunk_data_size, offset, fs.file_size)
	bytes_written, err := f.WriteAt(chunk.chunk_buffer[:chunk.chunk_data_size], int64(offset))
	if err != nil {
		log.Error("error writing file: ", err)
		return err
	}
	if bytes_written != chunk.chunk_data_size {
		err = fmt.Errorf("error writing file %d :  chunk %d wrote %d bytes (expected %d)",
			fs.file_id, chunk_idx, bytes_written, chunk.chunk_data_size)
		log.Error(err)
		return err
	}
	chunk.chunk_buffer = nil
	return nil
}

func (dec *FECFileDecoder) decode(input_folder string, output_dir string) {
	dec.all_files = make(map[uint64]*FileStatus)
	dec.read_chunks(input_folder, output_dir)
}

func main() {
	// Parse command line parameters.
	flag.Parse()

	encoder := newFECFileEncoder(*arg_num_data_shards, *arg_num_parity_shards, *arg_full_shard_size)
	log.Debug("before encode")
	encoder.encode(*arg_input_file, *arg_enc_out_dir)

	decoder := newFECFileDecoder(int64(*arg_chunk_timeout))
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
