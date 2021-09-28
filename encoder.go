package arson

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/bwmarrin/snowflake"
	"github.com/klauspost/reedsolomon"
	log "github.com/sirupsen/logrus"
)

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

func NewFECFileEncoder(data_shards, parity_shards, full_shard_size int) *FECFileEncoder {
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

func (enc *FECFileEncoder) send_chunks_to_io(writer io.Writer, cin chan *Chunk,
	done chan bool, fatal_errors chan error) {
	log.Debug("Starting to send chunks - listening on channel")
	for chunk := range cin {
		log.Debugf("send_chunks_to_io received chunk %d from channel", chunk.chunk_idx)
		for j := 0; j < chunk.num_total_shards; j++ {
			err := chunk.write_shard(j, writer)
			if err != nil {
				fatal_errors <- err
			}
		}
		chunk.free()
	}
	log.Debug("Finished sending chunks - channel closed")
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
	err := enc.encode_internal(filename, "", nil)
	if err != nil {
		return nil, err
	}
	return enc.get_all_shards(), nil
}

func (enc *FECFileEncoder) EncodeToFolder(filename string, output_dir string) error {
	err := enc.encode_internal(filename, output_dir, nil)
	if err != nil {
		return err
	}
	return nil
}

func (enc *FECFileEncoder) EncodeToStream(filename string, writer io.Writer) error {
	if writer == nil {
		return fmt.Errorf("EncodeToStream: entered nil writer")
	}
	return enc.encode_internal(filename, "", writer)
}

func (enc *FECFileEncoder) encode_internal(filename, output_dir string, writer io.Writer) error {
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
			} else if output_dir != "" {
				chunk.write_shards_to_folder(output_dir)
			}

		}(this_chunk)
	}
	// writer is nil when caller wants all shards in memory
	if writer != nil {
		go enc.send_chunks_to_io(writer, chunk_channel, write_done_channel, fatal_errors)
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
