package arson

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

const (
	shard_header_size    = 32
	shard_header_version = 0xA1
)

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

func (chunk *Chunk) write_shard(idx int, writer io.Writer) error {
	b := chunk.shard_header
	b[1] = byte(idx) // fill shard_idx in shard header
	_, err := writer.Write(b[:])
	if err != nil {
		return err
	}
	_, err = writer.Write(chunk.shards[idx])
	if err != nil {
		return err
	}
	log.Debugf("Finished writing chunk.shard %d.%d", chunk.chunk_idx, idx)
	return nil
}

func (chunk *Chunk) write_shards_to_folder(folder string) error {
	log.Debugf("write_shards_to_folder chunk %d", chunk.chunk_idx)
	for j := 0; j < chunk.num_total_shards; j++ {
		out_pattern := fmt.Sprintf("%d.%04d.%04d", chunk.file_id, chunk.chunk_idx, j)
		out_file := filepath.Join(folder, out_pattern)
		f, err := os.OpenFile(out_file, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		err = chunk.write_shard(j, f)
		if err != nil {
			return err
		}
	}
	chunk.free()
	return nil
}
