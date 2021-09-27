//go:build ignore
// +build ignore

// Copyright 2015, Klaus Post, see LICENSE for details.
//
// Simple decoder example.
//
// The decoder reverses the process of "simple-encoder.go"
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

func maindec() {
	all_files = make(map[uint64][]Chunk)
	read_chunks(*outDir)
	/*
		// Parse flags
		flag.Parse()
		args := flag.Args()
		if len(args) != 1 {
			fmt.Fprintf(os.Stderr, "Error: No filenames given\n")
			flag.Usage()
			os.Exit(1)
		}
		fname := args[0]

		// Create matrix
		enc, err := reedsolomon.New(*dataShards, *parShards)
		checkErr(err)

		// Create shards and load the data.
		shards := make([][]byte, *dataShards+*parShards)
		for i := range shards {
			infn := fmt.Sprintf("%s.%d", fname, i)
			fmt.Println("Opening", infn)
			shards[i], err = ioutil.ReadFile(infn)
			if err != nil {
				fmt.Println("Error reading file", err)
				shards[i] = nil
			}
		}

		// Verify the shards
		ok, err := enc.Verify(shards)
		if ok {
			fmt.Println("No reconstruction needed")
		} else {
			fmt.Println("Verification failed. Reconstructing data")
			err = enc.Reconstruct(shards)
			if err != nil {
				fmt.Println("Reconstruct failed -", err)
				os.Exit(1)
			}
			ok, err = enc.Verify(shards)
			if !ok {
				fmt.Println("Verification failed after reconstruction, data likely corrupted.")
				os.Exit(1)
			}
			checkErr(err)
		}

		// Join the shards and write them
		outfn := *outFile
		if outfn == "" {
			outfn = fname
		}

		fmt.Println("Writing data to", outfn)
		f, err := os.Create(outfn)
		checkErr(err)

		// We don't know the exact filesize.
		err = enc.Join(f, shards, len(shards[0])**dataShards)
		checkErr(err)*/
}
