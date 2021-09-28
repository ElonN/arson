package arson

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/reedsolomon"
	log "github.com/sirupsen/logrus"
)

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

type FECFileDecoder struct {
	all_files     map[uint64]*FileStatus
	chunk_timeout int64
	output_folder string
}

func NewFECFileDecoder(chunk_timeout int64, output_folder string) *FECFileDecoder {
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

	buffered_reader := bufio.NewReader(file)
	dec.Decode(buffered_reader)
	return nil
}

func (dec *FECFileDecoder) DecodeFromFolder(input_folder string) error {
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
