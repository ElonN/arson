package arson

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func BenchmarkFECDecode(b *testing.B) {

	files_path := filepath.Join(os.TempDir(), "fectest-469469927")
	decoder := NewFECFileDecoder(60, files_path)

	b.ReportAllocs()
	files, _ := ioutil.ReadDir(files_path)

	var sum_sizes int64

	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".encoded") {
			filename := filepath.Join(files_path, f.Name())
			file_to_decode, _ := os.Open(filename)
			f_stat, _ := file_to_decode.Stat()
			sum_sizes += f_stat.Size()
			decoder.Decode(file_to_decode)
			file_to_decode.Close()
		}
	}
	b.SetBytes(sum_sizes)

}
