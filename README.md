
![Image of Flame](https://static.wikia.nocookie.net/pam-rpg-system/images/3/30/Fire.png)

# arson
A package for applying [Reed-Solomon error correction](https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction) for reliable one-way data transfer.

It uses [Klaus Post's Reed Solomon package](https://github.com/klauspost/reedsolomon) and utilizes extra functionality essential for data transfer. The code was
inspired in a way by [xtaci's "kcp-go"](https://github.com/xtaci/kcp-go)'s implementation of "FEC Encoder/Decoder". 

From Klaus' documentation about shortcomings of his [simple encoder/decoder](https://github.com/klauspost/reedsolomon/tree/master/examples):

> Simple Encoder/Decoder Shortcomings:
>  * If the file size of the input isn't dividable by the number of data shards
>    the output will contain extra zeroes
> 
>  * If the shard numbers isn't the same for the decoder as in the
>    encoder, invalid output will be generated.
> 
>  * If values have changed in a shard, it cannot be reconstructed.
> 
>  * If two shards have been swapped, reconstruction will always fail.
>    You need to supply the shards in the same order as they were given to you.

This package takes large files, splits it to chunks with adjustable size, and applies reed-solomon encoding for each chunk.
Additionally it adds a header to each shard tracking important info such as file id, sizes, chunk index and shard index.
This header makes it possible to deduce all required information for reconstruction from the content of the shards only.
That means user can ignore ordering, file names, etc. - just transfer enough shards to the decoder.

The input data to encode can be of the following: 
* Stream from io.Reader (number of bytes should be given as argument)
* Read from file

The shards can be emitted in the following ways:
* Streamed to given io.writer
* Written to output directory
* Returned in-memory (not recommended for large files)

Common use-case is streaming shards over the network. For this case, shard size should be approximately MTU size.

## Code Snippet

On sender side -
```go

encoder := arson.NewFECFileEncoder(num_data_shards, num_parity_shards, max_shard_size)
var writer io.Writer = stream_to_output

// read from file
encoder.EncodeFileToStream(input_filepath, stream_to_output)

// read from stream (io.Reader)
encoder.EncodeToStream(reader_input, number_of_bytes, stream_to_output)
``` 
On receiver side -
```go
decoder := arson.NewFECFileDecoder(chunk_timeout, out_dir)
var reader io.Reader = input_stream
decoder.Decode(input_stream)
```

## Links
https://github.com/klauspost/reedsolomon -- Reed-Solomon Erasure Coding in Go

https://github.com/xtaci/kcp-go -- kcp-go is a Production-Grade Reliable-UDP library for golang
