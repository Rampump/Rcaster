use rcaster_core::ntrip::chunked::{ChunkError, ChunkedDecoder};

#[test]
fn decodes_chunked_body_split_at_every_boundary() {
    let input = b"4\r\nWiki\r\n5\r\npedia\r\n0\r\n\r\n";
    let mut decoder = ChunkedDecoder::new(1024);
    let mut output = Vec::new();

    for byte in input {
        decoder.decode(&[*byte], &mut output).expect("valid chunk");
    }

    assert_eq!(output, b"Wikipedia");
    assert!(decoder.is_finished());
}

#[test]
fn ignores_chunk_extensions_and_bounded_trailers() {
    let input = b"3;name=value\r\nabc\r\n0\r\nX-Checksum: ok\r\n\r\n";
    let mut decoder = ChunkedDecoder::new(1024);
    let mut output = Vec::new();

    decoder.decode(input, &mut output).expect("valid chunk");

    assert_eq!(output, b"abc");
    assert!(decoder.is_finished());
}

#[test]
fn rejects_invalid_chunk_terminator() {
    let input = b"3\r\nabcX0\r\n\r\n";
    let mut decoder = ChunkedDecoder::new(1024);
    let mut output = Vec::new();

    let error = decoder
        .decode(input, &mut output)
        .expect_err("bad terminator");

    assert_eq!(error, ChunkError::InvalidTerminator);
    assert_eq!(output, b"abc");
}

#[test]
fn rejects_invalid_chunk_size() {
    let input = b"nope\r\n";
    let mut decoder = ChunkedDecoder::new(1024);

    let error = decoder
        .decode(input, &mut Vec::new())
        .expect_err("bad chunk size");

    assert_eq!(error, ChunkError::InvalidChunkSize);
}

#[test]
fn rejects_trailers_over_the_configured_limit() {
    let input = b"0\r\nX-A: 1234\r\n\r\n";
    let mut decoder = ChunkedDecoder::new(8);

    let error = decoder
        .decode(input, &mut Vec::new())
        .expect_err("trailers too large");

    assert_eq!(error, ChunkError::TrailersTooLarge);
}

#[test]
fn rejects_bytes_after_the_terminal_trailer() {
    let input = b"0\r\n\r\nGET /C63\r\n";
    let mut decoder = ChunkedDecoder::new(1024);

    let error = decoder
        .decode(input, &mut Vec::new())
        .expect_err("trailing request");

    assert_eq!(error, ChunkError::TrailingBytes);
}
