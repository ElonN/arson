package arson

import (
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkDecode(b *testing.B) {

	num_data := 10
	num_parity := 3
	mtu := 1300
	encoder := NewFECFileEncoder(num_data, num_parity, mtu)

	temp_dir, _ := ioutil.TempDir("", "fectest-*")
	defer os.RemoveAll(temp_dir)

	fname, fsize, fhash := create_random_input_file(temp_dir)
	ioutil.WriteFile(fname+".hash", fhash, 0644)
	f, _ := os.Open(fname)

	encoded_fname := fname + ".encoded"
	f_encoded, _ := os.OpenFile(encoded_fname, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)

	_ = encoder.EncodeToStream(f, int64(fsize), f_encoded)
	f.Close()
	f_encoded.Close()

	f_info, _ := os.Stat(encoded_fname)
	b.SetBytes(f_info.Size())
	decoder := NewFECFileDecoder(60, temp_dir)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f_in, _ := os.Open(encoded_fname)
		decoder.Decode(f_in)
		f_in.Close()
	}
	b.StopTimer()
}
