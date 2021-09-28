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
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
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
	shard_header_size    = 32
	shard_header_version = 0xA1
	type_data            = 0xf1
	type_parity          = 0xf2
)

var (
	arg_num_data_shards   = flag.Int("ds", 10, "Number of data shards")
	arg_num_parity_shards = flag.Int("ps", 3, "Number of parity shards")
	arg_full_shard_size   = flag.Int("mss", 1300, "Maximum segment size to send (shard + header)")
	arg_chunk_timeout     = flag.Int("to", 60, "Timeout for receiving chunks (in seconds)")
	arg_input_file        = flag.String("f", "", "Input file")
	arg_enc_out_dir       = flag.String("eout", "", "Encoder output directory")
	arg_enc_out_file      = flag.String("eoutfile", "", "Encoder output file")
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
	chunk_size          int
	chunk_idx           int
	file_id             uint64
	file_size           int64
	total_chunks        int
	shards              [][]byte
	chunk_buffer        []byte
	is_finished         bool
	arrived_shards      int
	arrived_data_shards int
	timestamp           int64
	num_data_shards     int
	num_parity_shards   int
	num_total_shards    int
	shard_data_size     int
	zero_padding_length int
	shard_header        [shard_header_size]byte
}

type FileStatus struct {
	file_id             uint64
	file_size           int64
	total_chunks        int
	num_data_shards     int
	num_parity_shards   int
	num_total_shards    int
	shard_data_size     int
	chunk_size          int
	zero_padding_length int
	is_finished         bool
	is_failed           bool
	timestamp           int64
	output_folder       string
	chunks              []Chunk
}

type FECFileEncoder struct {
	num_data_shards    int
	num_parity_shards  int
	num_total_shards   int
	total_chunks       int
	full_shard_size    int
	shard_data_size    int
	total_chunk_buffer int
	type_data          byte
	type_parity        byte
	max_chunk_size     int
	idGen              *snowflake.Node
	chunks             []Chunk
}

func newFECFileEncoder(data_shards, parity_shards, full_shard_size int) *FECFileEncoder {
	enc := new(FECFileEncoder)
	enc.num_data_shards = data_shards
	enc.num_parity_shards = parity_shards
	enc.num_total_shards = data_shards + parity_shards
	enc.full_shard_size = full_shard_size
	enc.shard_data_size = full_shard_size - shard_header_size
	enc.total_chunk_buffer = enc.num_total_shards * enc.shard_data_size
	enc.type_data = 0xf1
	enc.type_parity = 0xf2
	enc.max_chunk_size = enc.shard_data_size * int(enc.num_data_shards)
	enc.idGen, _ = snowflake.NewNode(1)
	return enc
}

type FECFileDecoder struct {
	all_files     map[uint64]*FileStatus
	chunk_timeout int64
	output_folder string
}

func newFECFileDecoder(chunk_timeout int64, output_folder string) *FECFileDecoder {
	dec := new(FECFileDecoder)
	dec.all_files = make(map[uint64]*FileStatus)
	dec.chunk_timeout = chunk_timeout
	dec.output_folder = output_folder
	return dec
}

func parse_shard_header(b []byte) (file_id uint64, chunk_idx uint32, total_chunks uint32,
	chunk_size uint32, shard_idx byte, num_data_shards byte, num_parity_shards byte,
	shard_header_version byte, shard_data_size uint32, zero_padding_length uint32) {
	//
	//The header format:
	// |version(1B)|shard_idx(1B)|DS(1B)|PS(1B)|		 shard_data_size (4B) 		   |
	// |                      				file_id(8B)     						   |
	// |          chunk_idx (4B)         	   |       	total_chunks(4B)        	   |
	// |	 	 chunk_size(4B)		           |	 	zero_padding_length(4B)		   |
	//
	//
	shard_header_version = b[0]
	shard_idx = b[1]
	num_data_shards = b[2]
	num_parity_shards = b[3]
	shard_data_size = binary.LittleEndian.Uint32(b[4:])
	file_id = binary.LittleEndian.Uint64(b[8:])
	chunk_idx = binary.LittleEndian.Uint32(b[16:])
	total_chunks = binary.LittleEndian.Uint32(b[20:])
	chunk_size = binary.LittleEndian.Uint32(b[24:])
	zero_padding_length = binary.LittleEndian.Uint32(b[28:])
	return
}
func (chunk *Chunk) free() {
	chunk.shards = nil
	chunk.chunk_buffer = nil
}
func (chunk *Chunk) fill_shard_header() error {
	//
	//The header format:
	// |version(1B)|shard_idx(1B)|DS(1B)|PS(1B)|		 shard_data_size (4B) 		   |
	// |                      				file_id(8B)     						   |
	// |          chunk_idx (4B)         	   |       	total_chunks(4B)        	   |
	// |	 	 chunk_size(4B)		           |	 	zero_padding_length(4B)		   |
	//
	//
	if chunk.file_id == 0 || chunk.total_chunks == 0 || chunk.chunk_size == 0 ||
		chunk.num_data_shards == 0 {
		log.Errorf("fill_shard_header: file_id %d, file_size %d, chunk_idx %d, total_chunks %d, chunk_size %d, num_data_shards %d, num_parity_shards %d",
			chunk.file_id, chunk.file_size, chunk.chunk_idx, chunk.total_chunks, chunk.chunk_size,
			chunk.num_data_shards, chunk.num_parity_shards)
		return fmt.Errorf("tried to call mark_shard_header for an unintialized chunk")
	}
	b := chunk.shard_header[:]
	b[0] = byte(shard_header_version)
	// b[1] is shard_idx which we build dynamically
	b[2] = byte(chunk.num_data_shards)
	b[3] = byte(chunk.num_parity_shards)
	binary.LittleEndian.PutUint32(b[4:], uint32(chunk.shard_data_size))
	binary.LittleEndian.PutUint64(b[8:], uint64(chunk.file_id))
	binary.LittleEndian.PutUint32(b[16:], uint32(chunk.chunk_idx))
	binary.LittleEndian.PutUint32(b[20:], uint32(chunk.total_chunks))
	binary.LittleEndian.PutUint32(b[24:], uint32(chunk.chunk_size))
	binary.LittleEndian.PutUint32(b[28:], uint32(chunk.zero_padding_length))
	return nil
}

func Min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

func send_chunks_to_io(writer *bufio.Writer, cin chan *Chunk,
	done chan bool, fatal_errors chan error) {
	log.Debug("Starting to send chunks - listening on channel")
	for chunk := range cin {
		log.Debugf("send_chunks_to_io received chunk %d from channel", chunk.chunk_idx)
		for j := 0; j < chunk.num_total_shards; j++ {
			b := chunk.shard_header
			b[1] = byte(j) // fill shard_idx in shard header
			_, err := writer.Write(b[:])
			if err != nil {
				fatal_errors <- err
			}
			_, err = writer.Write(chunk.shards[j])
			if err != nil {
				fatal_errors <- err
			}
			log.Debugf("Finished writing chunk.shard %d.%d", chunk.chunk_idx, j)
		}
		chunk.free()
	}
	log.Debug("Finished sending chunks - channel closed")
	writer.Flush()
	done <- true
}

func (enc *FECFileEncoder) get_all_shards() [][]byte {
	all_shards := make([][]byte, 0)
	for i := range enc.chunks {
		start_idx := i * enc.num_total_shards
		end_idx := (i + 1) * enc.num_total_shards
		copy(all_shards[start_idx:end_idx], enc.chunks[i].shards)
	}
	return all_shards
}

func (enc *FECFileEncoder) Encode(filename string) ([][]byte, error) {
	err := enc.EncodeStream(filename, nil)
	if err != nil {
		return nil, err
	}
	return enc.get_all_shards(), nil
}

func (enc *FECFileEncoder) EncodeStream(filename string, writer *bufio.Writer) error {
	file_id := uint64(enc.idGen.Generate())
	log.Debug("Opening ", filename)
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		return err
	}

	chunk_size := enc.max_chunk_size
	filesize := fileinfo.Size()
	log.Debug("File size of ", filename, " is ", filesize)
	log.Debug("Chunk size is ", chunk_size)
	// Number of go routines we need to spawn.
	enc.total_chunks = int(math.Ceil(float64(filesize) / float64(chunk_size)))
	total_chunks := enc.total_chunks
	log.Debug("num_chunks is ", total_chunks)
	enc.chunks = make([]Chunk, total_chunks)
	chunks := enc.chunks

	for i := 0; i < total_chunks; i++ {
		chunks[i].chunk_size = chunk_size
		chunks[i].chunk_idx = i
		chunks[i].file_id = file_id
		chunks[i].total_chunks = total_chunks
		chunks[i].file_size = filesize
		chunks[i].chunk_buffer = make([]byte, enc.num_total_shards*enc.shard_data_size)
		chunks[i].num_data_shards = enc.num_data_shards
		chunks[i].num_parity_shards = enc.num_parity_shards
		chunks[i].num_total_shards = enc.num_total_shards
		chunks[i].shard_data_size = enc.shard_data_size
		chunks[i].zero_padding_length = total_chunks*chunk_size - int(filesize)

		err = chunks[i].fill_shard_header()
		if err != nil {
			return err
		}
	}

	// Make channels to pass fatal errors in WaitGroup
	fatal_errors := make(chan error)
	wgDone := make(chan bool)
	chunk_channel := make(chan *Chunk)
	write_done_channel := make(chan bool)

	var wg sync.WaitGroup
	wg.Add(total_chunks)

	// start buffered read
	// first - sequentially reads chunks form file
	// then - spawns goroutines and encodes chunks concurrently
	buffered_reader := bufio.NewReaderSize(file, chunk_size)

	log.Debug("Starting chunk goroutines")
	for i := 0; i < total_chunks; i++ {
		this_chunk := &chunks[i]

		//				 |------DATA SHARDS-----------| |----PARITY SHARDS-----|
		// chunk_buffer = data_from_file + zero_padding + space_for_parity_shards

		size_to_read := this_chunk.chunk_size
		if i == total_chunks-1 {
			// last chunk has partial data
			// for last chunk:  chunk_size = last_chunk_data + zero_padding
			size_to_read = this_chunk.chunk_size - this_chunk.zero_padding_length
		}
		bytes_read, err := io.ReadFull(buffered_reader, this_chunk.chunk_buffer[:size_to_read])
		if err != nil {
			return err
		}
		log.Debug("Read ", bytes_read, " / ", size_to_read, " bytes for chunk ", i)

		// Each of these go routines will encode a chunk and create the parity shards
		go func(chunk *Chunk) {
			defer wg.Done()
			log.Debug("Inside chunk goroutine number ", chunk.chunk_idx)

			// "shards" are slices from chunk_buffer
			chunk.shards = make([][]byte, enc.num_total_shards)
			for j := 0; j < enc.num_total_shards; j++ {
				idx_start := enc.shard_data_size * j
				idx_end := idx_start + enc.shard_data_size
				chunk.shards[j] = chunk.chunk_buffer[idx_start:idx_end]
			}

			// Create encoding matrix
			encoder, err := reedsolomon.New(enc.num_data_shards, enc.num_parity_shards)
			if err != nil {
				fatal_errors <- err
			}
			// perform the encode - create parity shards
			err = encoder.Encode(chunk.shards)
			if err != nil {
				fatal_errors <- err
			}
			// writer is nil when caller wants all shards in memory
			if writer != nil {
				chunk_channel <- chunk
			}

		}(this_chunk)
	}
	// writer is nil when caller wants all shards in memory
	if writer != nil {
		go send_chunks_to_io(writer, chunk_channel, write_done_channel, fatal_errors)

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
		log.Debug("waitgroup finished")
		if writer == nil { // writer is nil when caller wants all shards in memory
			// we got all shards in memory, now return
			return nil
		}
		// if we have a writer, we need to wait for it to finish
		// first, signal the writer goroutine that no more chunks are left
		close(chunk_channel)
		break
	case err := <-fatal_errors:
		log.Debug("got fatal error from channel")
		wg.Wait()
		return err
	}

	// wait for writer goroutine
	select {
	case <-write_done_channel: // write goroutine finished
		log.Debug("got finish signal from writing routine")
		break
	case err := <-fatal_errors:
		log.Debug("got fatal error from channel")
		return err
	}

	return nil
}

func (fs *FileStatus) free_file() {

	for i := range fs.chunks {
		fs.chunks[i].shards = nil
		fs.chunks[i].chunk_buffer = nil
	}

}

func (dec *FECFileDecoder) put_shard(shard []byte) {
	// read header from beginning
	file_id, chunk_idx, total_chunks, chunk_size,
		shard_idx, num_data_shards, num_parity_shards,
		version, shard_data_size, zero_padding_length := parse_shard_header(shard[:shard_header_size])
	log.Debug("shard header is ", file_id, chunk_idx, total_chunks, chunk_size,
		shard_idx, num_data_shards, num_parity_shards,
		version, shard_data_size, zero_padding_length)

	if version != shard_header_version {
		log.Warn("unknown header version")
		return
	}

	// check if new file
	if _, ok := dec.all_files[file_id]; !ok {
		// NEW FILE - initialize chunk array and sync object
		dec.all_files[file_id] = new(FileStatus)
		this_file := dec.all_files[file_id]
		this_file.chunks = make([]Chunk, total_chunks)
		this_file.num_data_shards = int(num_data_shards)
		this_file.num_parity_shards = int(num_parity_shards)
		this_file.num_total_shards = int(num_data_shards + num_parity_shards)
		this_file.file_id = file_id
		this_file.total_chunks = int(total_chunks)
		this_file.chunk_size = int(chunk_size)
		this_file.zero_padding_length = int(zero_padding_length)
		this_file.shard_data_size = int(shard_data_size)
		this_file.output_folder = dec.output_folder
		this_file.file_size = int64(total_chunks*chunk_size - zero_padding_length)

		// initialize all chunks for this file
		for j := range this_file.chunks {
			chunk := &this_file.chunks[j]
			// chunk_buffer holds the actual data of the chunk (without headers)
			// NOTE: chunk_buffer is padded with zeros
			// exact length of zero padding is in chunk.zero_padding_length
			chunk.chunk_idx = j
			chunk.chunk_size = this_file.chunk_size
			chunk.zero_padding_length = this_file.zero_padding_length
			chunk.file_id = this_file.file_id
			chunk.file_size = this_file.file_size
			chunk.num_data_shards = this_file.num_data_shards
			chunk.num_parity_shards = this_file.num_parity_shards
			chunk.total_chunks = this_file.total_chunks

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
	if this_chunk.timestamp == 0 { // newly arrived chunk
		this_chunk.timestamp = time.Now().Unix()
		this_chunk.chunk_buffer = make([]byte,
			int(this_file.num_total_shards)*this_file.shard_data_size)
		this_chunk.shards = make([][]byte, int(this_file.num_total_shards))
		// initialize all shards for this chunk
		// each shard points to its slice in the chunk_buffer
		for k := 0; k < int(this_file.num_total_shards); k++ {
			// Default is slice with zero length
			// This is important for reconstruction, see docs for "reedsolomon.Reconstruct"
			idx_start := this_file.shard_data_size * k
			this_chunk.shards[k] = this_chunk.chunk_buffer[idx_start:idx_start]
		}
	}
	// if now is after timeout for chunk
	if time.Now().Unix() > this_chunk.timestamp+dec.chunk_timeout {
		log.Warnf("shard arrived for timed out chunk %d of file %d", chunk_idx, file_id)
		this_file.is_failed = true
		this_file.free_file()
		return
	}

	// copy shard data to chunk_buffer
	idx_start := int(shard_idx) * this_file.shard_data_size
	idx_end := idx_start + this_file.shard_data_size
	copy(this_chunk.chunk_buffer[idx_start:idx_end], shard[shard_header_size:])

	// make shard slice point to where the data is in chunk_buffer
	this_chunk.shards[shard_idx] = this_chunk.chunk_buffer[idx_start:idx_end]

	// keep track how many shards have arrived
	this_chunk.arrived_shards++
	if shard_idx < num_data_shards {
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
			file_id, chunk_idx, dec.output_folder)
		this_file.write_chunk(int(chunk_idx))
		this_chunk.is_finished = true
	}

}

func (dec *FECFileDecoder) Decode(r io.Reader) error {
	b := make([]byte, shard_header_size)
	for {
		_, err := io.ReadFull(r, b[:1])
		if err != nil {
			if err == io.EOF {
				log.Debug("Decode: reached EOF")
				return nil
			}
			return err
		}
		if b[0] != shard_header_version {
			err = fmt.Errorf("unknown shard header version")
			log.Error(err)
			return err
		}
		num_read, err := io.ReadFull(r, b[1:shard_header_size])
		if err != nil {
			log.Errorf("Decode : ReadFull - header read is %d / %d", num_read, shard_header_size-1)
			return err
		}
		shard_size := binary.LittleEndian.Uint32(b[4:])
		shard_buf := make([]byte, shard_header_size+shard_size)
		copy(shard_buf[:shard_header_size], b)
		num_read, err = io.ReadFull(r, shard_buf[shard_header_size:])
		if err != nil {
			log.Errorf("Decode : ReadFull - shard read is %d / %d", num_read, shard_size)
			return err
		}
		log.Debug("Decode: putting shard")
		dec.put_shard(shard_buf)
	}
}
func (dec *FECFileDecoder) decode_from_file(filename string) error {
	log.Debug("Opening ", filename)
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// start buffered read
	// first - sequentially reads chunks form file
	// then - spawns goroutines and encodes chunks concurrently
	buffered_reader := bufio.NewReader(file)
	dec.Decode(buffered_reader)
	return nil
}

func (dec *FECFileDecoder) decode_from_folder(input_folder string) error {
	files, err := ioutil.ReadDir(input_folder)
	if err != nil {
		return err
	}

	for _, f := range files {
		filename := filepath.Join(input_folder, f.Name())
		dec.decode_from_file(filename)
	}
	return nil
}

func (fs *FileStatus) write_chunk(chunk_idx int) error {
	outfile_basename := fmt.Sprintf("%d.out", fs.file_id)
	outfile_fullname := filepath.Join(fs.output_folder, outfile_basename)
	chunk := fs.chunks[chunk_idx]
	offset := chunk_idx * chunk.chunk_size

	f, err := os.OpenFile(outfile_fullname, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	log.Debugf("chunk WriteAt: file %s, chunk idx: %d,chunk_size: %d, offset: %d, filesize %d",
		outfile_fullname, chunk_idx, chunk.chunk_size, offset, fs.file_size)
	size_to_write := chunk.chunk_size
	if chunk_idx == chunk.total_chunks-1 {
		// last chunk has partial data
		// for last chunk:  chunk_size = last_chunk_data + zero_padding
		size_to_write = chunk.chunk_size - chunk.zero_padding_length
	}

	bytes_written, err := f.WriteAt(chunk.chunk_buffer[:size_to_write], int64(offset))
	if err != nil {
		log.Error("error writing file: ", err)
		return err
	}
	if bytes_written != size_to_write {
		err = fmt.Errorf("error writing file %d :  chunk %d wrote %d bytes (expected %d)",
			fs.file_id, chunk_idx, bytes_written, size_to_write)
		log.Error(err)
		return err
	}
	chunk.chunk_buffer = nil
	return nil
}

func main() {
	// Parse command line parameters.
	flag.Parse()

	out_file_encoder, err := os.OpenFile(*arg_enc_out_file,
		os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error(err)
		return
	}

	buffered_writer := bufio.NewWriter(out_file_encoder)

	encoder := newFECFileEncoder(*arg_num_data_shards, *arg_num_parity_shards, *arg_full_shard_size)
	log.Debug("before encode")
	encoder.EncodeStream(*arg_input_file, buffered_writer)
	out_file_encoder.Close()

	decoder := newFECFileDecoder(int64(*arg_chunk_timeout), *arg_dec_out_dir)
	log.Debug("before decode")
	//decoder.decode_from_folder(*arg_enc_out_dir)
	decoder.decode_from_file(*arg_enc_out_file)
	log.Debug("finish")
}
