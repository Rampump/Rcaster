use std::{fmt, str};

const MAX_CHUNK_LINE_BYTES: usize = 1024;
const MAX_TRAILER_LINE_BYTES: usize = 4096;

pub struct ChunkedDecoder {
    buffer: Vec<u8>,
    state: State,
    max_trailer_bytes: usize,
    trailer_bytes: usize,
}

#[derive(Clone, Copy)]
enum State {
    Size,
    Data(usize),
    DataTerminator,
    Trailers,
    Done,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChunkError {
    ChunkLineTooLong,
    InvalidChunkSize,
    InvalidChunkLine,
    InvalidTerminator,
    TrailersTooLarge,
    TrailingBytes,
}

impl ChunkedDecoder {
    pub fn new(max_trailer_bytes: usize) -> Self {
        Self {
            buffer: Vec::new(),
            state: State::Size,
            max_trailer_bytes,
            trailer_bytes: 0,
        }
    }

    pub fn decode(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<(), ChunkError> {
        if matches!(self.state, State::Done) {
            return if input.is_empty() {
                Ok(())
            } else {
                Err(ChunkError::TrailingBytes)
            };
        }
        self.buffer.extend_from_slice(input);

        loop {
            match self.state {
                State::Size => {
                    let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') else {
                        if self.buffer.len() > MAX_CHUNK_LINE_BYTES {
                            return Err(ChunkError::ChunkLineTooLong);
                        }
                        break;
                    };
                    let line = trim_line_ending(&self.buffer[..newline]);
                    if line.len() > MAX_CHUNK_LINE_BYTES {
                        return Err(ChunkError::ChunkLineTooLong);
                    }
                    let size = parse_chunk_size(line)?;
                    self.buffer.drain(..=newline);
                    self.state = if size == 0 {
                        State::Trailers
                    } else {
                        State::Data(size)
                    };
                }
                State::Data(remaining) => {
                    if remaining == 0 {
                        self.state = State::DataTerminator;
                        continue;
                    }
                    if self.buffer.is_empty() {
                        break;
                    }
                    let take = remaining.min(self.buffer.len());
                    output.extend_from_slice(&self.buffer[..take]);
                    self.buffer.drain(..take);
                    self.state = State::Data(remaining - take);
                }
                State::DataTerminator => {
                    let Some(first) = self.buffer.first() else {
                        break;
                    };
                    let consumed = match *first {
                        b'\n' => 1,
                        b'\r' if self.buffer.get(1) == Some(&b'\n') => 2,
                        b'\r' => break,
                        _ => return Err(ChunkError::InvalidTerminator),
                    };
                    self.buffer.drain(..consumed);
                    self.state = State::Size;
                }
                State::Trailers => {
                    let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') else {
                        if self.buffer.len() > MAX_TRAILER_LINE_BYTES
                            || self.trailer_bytes.saturating_add(self.buffer.len())
                                > self.max_trailer_bytes
                        {
                            return Err(ChunkError::TrailersTooLarge);
                        }
                        break;
                    };
                    let line = trim_line_ending(&self.buffer[..newline]);
                    if line.is_empty() {
                        self.buffer.drain(..=newline);
                        if !self.buffer.is_empty() {
                            return Err(ChunkError::TrailingBytes);
                        }
                        self.state = State::Done;
                        break;
                    }
                    if line.len() > MAX_TRAILER_LINE_BYTES {
                        return Err(ChunkError::TrailersTooLarge);
                    }
                    self.trailer_bytes =
                        self.trailer_bytes.saturating_add(newline.saturating_add(1));
                    if self.trailer_bytes > self.max_trailer_bytes {
                        return Err(ChunkError::TrailersTooLarge);
                    }
                    self.buffer.drain(..=newline);
                }
                State::Done => break,
            }
        }
        Ok(())
    }

    pub fn is_finished(&self) -> bool {
        matches!(self.state, State::Done)
    }
}

impl fmt::Display for ChunkError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChunkLineTooLong => formatter.write_str("chunk size line is too long"),
            Self::InvalidChunkSize => formatter.write_str("invalid chunk size"),
            Self::InvalidChunkLine => formatter.write_str("invalid chunk extension line"),
            Self::InvalidTerminator => formatter.write_str("invalid chunk terminator"),
            Self::TrailersTooLarge => formatter.write_str("chunk trailers are too large"),
            Self::TrailingBytes => formatter.write_str("bytes follow the chunked body"),
        }
    }
}

impl std::error::Error for ChunkError {}

fn parse_chunk_size(line: &[u8]) -> Result<usize, ChunkError> {
    if line
        .iter()
        .any(|byte| *byte == 0 || (*byte < b' ' && *byte != b'\t') || *byte == 0x7f)
    {
        return Err(ChunkError::InvalidChunkLine);
    }
    let size = line.split(|byte| *byte == b';').next().unwrap_or_default();
    let size = trim_ascii(size);
    if size.is_empty() {
        return Err(ChunkError::InvalidChunkSize);
    }
    let size = str::from_utf8(size).map_err(|_| ChunkError::InvalidChunkSize)?;
    let size = usize::from_str_radix(size, 16).map_err(|_| ChunkError::InvalidChunkSize)?;
    Ok(size)
}

fn trim_line_ending(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn trim_ascii(mut value: &[u8]) -> &[u8] {
    while value
        .first()
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
    {
        value = &value[1..];
    }
    while value
        .last()
        .is_some_and(|byte| matches!(*byte, b' ' | b'\t'))
    {
        value = &value[..value.len() - 1];
    }
    value
}
