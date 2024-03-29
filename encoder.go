package arson

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

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
	max_chunk_size     int
	idGen              *snowflake.Node
	chunks             []Chunk
}

// Returns a new File FEC Encoder.
// The encoder receives the number of data shards and parity shards as in input, as well
// as the maximum size of each shard.
// The maximum shard size must be larger than 32 byte.
func NewFECFileEncoder(data_shards, parity_shards, max_shard_size int) *FECFileEncoder {
	if max_shard_size <= shard_header_size {
		return nil
	}
	enc := new(FECFileEncoder)
	enc.num_data_shards = data_shards
	enc.num_parity_shards = parity_shards
	enc.num_total_shards = data_shards + parity_shards
	enc.full_shard_size = max_shard_size
	enc.shard_data_size = max_shard_size - shard_header_size
	enc.total_chunk_buffer = enc.num_total_shards * enc.shard_data_size
	enc.max_chunk_size = enc.shard_data_size * int(enc.num_data_shards)
	random_snowflake_node := time.Now().Unix() % (int64(math.Pow(2, float64(snowflake.NodeBits))))
	enc.idGen, _ = snowflake.NewNode(random_snowflake_node)
	return enc
}

func (enc *FECFileEncoder) send_chunks_to_io(writer io.Writer, cin chan *Chunk,
	done chan bool, fatal_errors chan error) {
	log.Debug("Starting to send chunks - listening on channel")
	buf_writer := bufio.NewWriterSize(writer, enc.full_shard_size)
	for chunk := range cin {
		log.Debugf("send_chunks_to_io received chunk %d from channel", chunk.chunk_idx)
		for j := 0; j < chunk.num_total_shards; j++ {
			err := chunk.write_shard(j, buf_writer)
			if err != nil {
				fatal_errors <- err
			}
		}
		buf_writer.Flush()
		chunk.free()
	}
	log.Debug("Finished sending chunks - channel closed")
	done <- true
}

func (enc *FECFileEncoder) get_all_shards() [][]byte {
	all_shards := make([][]byte, enc.num_total_shards*enc.total_chunks)
	for i := range enc.chunks {
		start_idx := i * enc.num_total_shards
		end_idx := (i + 1) * enc.num_total_shards
		copy(all_shards[start_idx:end_idx], enc.chunks[i].shards)
	}
	return all_shards
}

// Encodes stream from io.reader and returns slice that has all shards
// The resulting shards will be at most max_shard_size and will
// have all data needed for reconstruction embedded in their content.
// All shards will remain in memory so for large files consider using
// other methods - EncodeToStream or EncodeToFolder
func (enc *FECFileEncoder) Encode(r io.Reader, num_bytes int64) ([][]byte, error) {
	err := enc.encode_internal(r, num_bytes, "", nil)
	if err != nil {
		return nil, err
	}
	return enc.get_all_shards(), nil
}

// Encodes file <filename> and returns slice that has all shards
// The resulting shards will be at most max_shard_size and will
// have all data needed for reconstruction embedded in their content.
// All shards will remain in memory so for large files consider using
// other methods - EncodeToStream or EncodeToFolder
func (enc *FECFileEncoder) EncodeFile(filename string) ([][]byte, error) {
	err := enc.encode_file_internal(filename, "", nil)
	if err != nil {
		return nil, err
	}
	return enc.get_all_shards(), nil
}

// Encodes stream from io.reader and saves it's shards to output_dir
// The resulting shards will be at most max_shard_size and will
// have all data needed for reconstruction embedded in their content.
// i.e their filenames are not necesarry and can be changed.
// File will be read and written chunk by chunk so that even large files will
// have reasonable memory consumption
func (enc *FECFileEncoder) EncodeToFolder(r io.Reader, num_bytes int64, output_dir string) error {
	err := enc.encode_internal(r, num_bytes, output_dir, nil)
	if err != nil {
		return err
	}
	return nil
}

// Encodes file <filename> and saves it's shards to output_dir
// The resulting shards will be at most max_shard_size and will
// have all data needed for reconstruction embedded in their content.
// i.e their filenames are not necesarry and can be changed.
// File will be read and written chunk by chunk so that even large files will
// have reasonable memory consumption
func (enc *FECFileEncoder) EncodeFileToFolder(filename string, output_dir string) error {
	err := enc.encode_file_internal(filename, output_dir, nil)
	if err != nil {
		return err
	}
	return nil
}

// Encodes stream from io.reader to shards and streams them to io.writer.
// The resulting shards will be at most max_shard_size and will
// have all data needed for reconstruction embedded in their content.
// File will be read and streamed chunk by chunk so that even large files will
// have reasonable memory consumption
func (enc *FECFileEncoder) EncodeToStream(r io.Reader, num_bytes int64, writer io.Writer) error {
	if writer == nil {
		return fmt.Errorf("EncodeToStream: entered nil writer")
	}
	return enc.encode_internal(r, num_bytes, "", writer)
}

// Encodes file <filename> to shards and streams them to io.writer.
// The resulting shards will be at most max_shard_size and will
// have all data needed for reconstruction embedded in their content.
// File will be read and streamed chunk by chunk so that even large files will
// have reasonable memory consumption
func (enc *FECFileEncoder) EncodeFileToStream(filename string, writer io.Writer) error {
	if writer == nil {
		return fmt.Errorf("EncodeToStream: entered nil writer")
	}
	return enc.encode_file_internal(filename, "", writer)
}

func (enc *FECFileEncoder) encode_file_internal(filename string, output_dir string, writer io.Writer) error {

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
	filesize := fileinfo.Size()
	log.Debug("File size of ", filename, " is ", filesize)

	return enc.encode_internal(file, filesize, output_dir, writer)
}

// This function will read sequentially from io.reader and fill chunk data.
// After a whole chunk is filled, it will fire up a goroutine that computes the
// reed-solomon encoding concurrently.
// The main function will continue reading chunks and starting goroutines.
// Meanwhile, the output goroutine is running in the backgroung waiting for the
// encoding goroutines to send ready buffers to output to the writer.
// The signals and datav are sent through channels.
func (enc *FECFileEncoder) encode_internal(r io.Reader, num_bytes int64, output_dir string, writer io.Writer) error {
	file_id := uint64(enc.idGen.Generate())

	chunk_size := enc.max_chunk_size
	log.Debug("Size is ", num_bytes)
	log.Debug("Chunk size is ", chunk_size)

	// total_chunks is simply the file size divided by chunk size
	enc.total_chunks = int(math.Ceil(float64(num_bytes) / float64(chunk_size)))
	total_chunks := enc.total_chunks
	log.Debug("num_chunks is ", total_chunks)

	// make channels to communicate with io writer goroutine
	fatal_errors := make(chan error)
	chunk_channel := make(chan *Chunk)
	write_done_channel := make(chan bool)

	// writer is nil when caller wants all shards in memory
	if writer != nil {
		// spawn io worker goroutine
		go enc.send_chunks_to_io(writer, chunk_channel, write_done_channel, fatal_errors)
	}

	// chunks are saved in the encoder context
	enc.chunks = make([]Chunk, total_chunks)
	chunks := enc.chunks

	// we are going to spawn <total_chunks> goroutines that encode chunks
	// to wait for all of them to finish, we create a wait group
	wg_done := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(total_chunks)

	// each chunk should be rather independent from encoder context, so we will
	// initialize all relevent data to chunk attributes
	for i := 0; i < total_chunks; i++ {
		chunk := &chunks[i]
		chunk.chunk_size = chunk_size
		chunk.chunk_idx = i
		chunk.file_id = file_id
		chunk.total_chunks = total_chunks
		chunk.file_size = num_bytes
		chunk.chunk_buffer = make([]byte, enc.num_total_shards*enc.shard_data_size)
		chunk.num_data_shards = enc.num_data_shards
		chunk.num_parity_shards = enc.num_parity_shards
		chunk.num_total_shards = enc.num_total_shards
		chunk.shard_data_size = enc.shard_data_size
		chunk.zero_padding_length = total_chunks*chunk_size - int(num_bytes)

		err := chunk.fill_shard_header()
		if err != nil {
			return err
		}

		//  read data and fill into shards in chunk buffer
		//  after filled out, chunk_buffer will look as following:
		//  some of the data shards will be fully occupied with real data, and if zero padding is needed
		//  in order to fill exactly num_data_shards * shard_size, there will be at least one data
		//  shard with data + padding, and possibly some data shards filled only by zero padding

		//				 |------DATA SHARDS-----------| |----PARITY SHARDS-----|
		// chunk_buffer = data_from_file + zero_padding + space_for_parity_shards

		size_to_read := chunk.chunk_size
		if i == total_chunks-1 {
			// last chunk has partial data
			// for last chunk:  chunk_size = last_chunk_data + zero_padding
			// hence size to read is the following -
			size_to_read = chunk.chunk_size - chunk.zero_padding_length
		}
		bytes_read, err := io.ReadFull(r, chunk.chunk_buffer[:size_to_read])
		if err != nil {
			return err
		}
		log.Debug("Read ", bytes_read, " / ", size_to_read, " bytes for chunk ", i)

		// "shards" are just slices from chunk_buffer
		chunk.shards = make([][]byte, enc.num_total_shards)
		for j := 0; j < enc.num_total_shards; j++ {
			idx_start := enc.shard_data_size * j
			idx_end := idx_start + enc.shard_data_size
			chunk.shards[j] = chunk.chunk_buffer[idx_start:idx_end]
		}

		// Each of these go routines will encode a chunk and create the parity shards
		go func(c *Chunk) {
			defer wg.Done()

			// create encoding matrix
			encoder, err := reedsolomon.New(enc.num_data_shards, enc.num_parity_shards)
			if err != nil {
				fatal_errors <- err
			}
			// perform the encode - create parity shards
			err = encoder.Encode(c.shards)
			if err != nil {
				fatal_errors <- err
			}
			// writer is nil when caller wants all shards in memory and not to use streaming
			if writer != nil {
				// send chunk to io worker goroutine
				chunk_channel <- c
			} else if output_dir != "" {
				c.write_shards_to_folder(output_dir)
			} // else: shards will remain in memory

		}(chunk)
	}

	// final goroutine to wait until WaitGroup is done
	go func() {
		wg.Wait()
		close(wg_done)
	}()

	// Wait until either WaitGroup is done or an error is received through the channel
	select {
	case <-wg_done:
		// carry on
		log.Debug("waitgroup finished")
		if writer == nil { // writer is nil when caller wants all shards in memory
			// we got all shards in memory, now return
			return nil
		}
		// if we have an io worker, we need to wait for it to finish
		// first, signal the  io worker goroutine that no more chunks are left
		close(chunk_channel)
		break
	case err := <-fatal_errors:
		log.Error("got fatal error from channel")
		wg.Wait()
		return err
	}

	// now wait for writer goroutine to finish
	select {
	case <-write_done_channel: // write goroutine finished
		log.Info("finished writing file ", file_id)
		break
	case err := <-fatal_errors:
		log.Debug("got fatal error from channel")
		return err
	}

	return nil
}
