package arson

import (
	"crypto/sha256"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"
)

const (
	Megabyte = 1 << 20
)

type my_reader struct{}

func (r *my_reader) Read(p []byte) (n int, err error) {
	return rand.Read(p)
}

func get_file_hash(file string) []byte {
	// calc hash
	f_hash, _ := os.Open(file)
	defer f_hash.Close()

	h := sha256.New()
	_, _ = io.Copy(h, f_hash)
	return h.Sum(nil)
}

func create_random_input_file(dir string) (name string, size int, hash []byte) {
	rand.Seed(time.Now().UnixNano())
	var r my_reader
	min := 2 * Megabyte
	max := 10 * Megabyte
	size = rand.Intn(max-min+1) + min

	// fill random file
	f, _ := ioutil.TempFile(dir, "file-*.in")
	name = f.Name()
	_, _ = io.CopyN(f, &r, int64(size))
	f.Close()

	hash = get_file_hash(name)
	return
}

/*
func BenchmarkFECDecode(b *testing.B) {

	decoder := NewFECFileDecoder(int64(*arg_chunk_timeout), *arg_dec_out_dir)
	for i := 0; i < b.N; i++ {
		stream_to_decode, err := os.Open(*arg_enc_out_file)
		if err != nil {
			log.Error(err)
			return
		}
		defer stream_to_decode.Close()

		var reader io.Reader = stream_to_decode

		decoder.Decode(reader)

	}
}*/

func BenchmarkEncodeToFolder(b *testing.B) {

	num_data := 10
	num_parity := 3
	mtu := 1300
	encoder := NewFECFileEncoder(num_data, num_parity, mtu)

	temp_dir, _ := ioutil.TempDir("", "fectest-*")
	defer os.RemoveAll(temp_dir)

	fname, fsize, fhash := create_random_input_file(temp_dir)
	b.SetBytes(int64(fsize))
	ioutil.WriteFile(fname+".hash", fhash, 0644)
	encoded_fname := fname + ".encoded"

	_ = os.Mkdir(encoded_fname, 0644)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, _ := os.Open(fname)
		encoder.EncodeToFolder(f, int64(fsize), encoded_fname)
		f.Close()
	}
	b.StopTimer()
}

func BenchmarkEncodeToFile(b *testing.B) {

	num_data := 10
	num_parity := 3
	mtu := 1300
	encoder := NewFECFileEncoder(num_data, num_parity, mtu)

	temp_dir, _ := ioutil.TempDir("", "fectest-*")
	defer os.RemoveAll(temp_dir)

	fname, fsize, fhash := create_random_input_file(temp_dir)
	b.SetBytes(int64(fsize))
	ioutil.WriteFile(fname+".hash", fhash, 0644)
	encoded_fname := fname + ".encoded"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f_in, _ := os.Open(fname)
		f_out, _ := os.OpenFile(encoded_fname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		encoder.EncodeToStream(f_in, int64(fsize), f_out)
		f_in.Close()
		f_out.Close()
	}
	b.StopTimer()
}

func BenchmarkEncode(b *testing.B) {

	num_data := 10
	num_parity := 3
	mtu := 1300
	encoder := NewFECFileEncoder(num_data, num_parity, mtu)

	temp_dir, _ := ioutil.TempDir("", "fectest-*")
	defer os.RemoveAll(temp_dir)

	fname, fsize, fhash := create_random_input_file(temp_dir)
	b.SetBytes(int64(fsize))
	ioutil.WriteFile(fname+".hash", fhash, 0644)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, _ := os.Open(fname)
		_, _ = encoder.Encode(f, int64(fsize))
		f.Close()
	}
	b.StopTimer()
}
