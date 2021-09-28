package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/ElonN/arson"
	log "github.com/sirupsen/logrus"
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

func example_encode_decode_to_stream() {
	encoder := arson.NewFECFileEncoder(*arg_num_data_shards, *arg_num_parity_shards, *arg_full_shard_size)
	decoder := arson.NewFECFileDecoder(int64(*arg_chunk_timeout), *arg_dec_out_dir)

	write_stream, err := os.OpenFile(*arg_enc_out_file,
		os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error(err)
		return
	}
	defer write_stream.Close()

	var writer io.Writer = write_stream

	encoder.EncodeToStream(*arg_input_file, writer)

	stream_to_decode, err := os.Open(*arg_enc_out_file)
	if err != nil {
		log.Error(err)
		return
	}
	defer stream_to_decode.Close()

	var reader io.Reader = stream_to_decode

	decoder.Decode(reader)
}

func example_encode_decode_to_folder() {
	encoder := arson.NewFECFileEncoder(*arg_num_data_shards, *arg_num_parity_shards, *arg_full_shard_size)
	decoder := arson.NewFECFileDecoder(int64(*arg_chunk_timeout), *arg_dec_out_dir)

	encoder.EncodeToFolder(*arg_input_file, *arg_enc_out_dir)
	decoder.DecodeFromFolder(*arg_enc_out_dir)
}

func main() {
	// Parse command line parameters.
	flag.Parse()

	example_encode_decode_to_stream()

	example_encode_decode_to_folder()
}
