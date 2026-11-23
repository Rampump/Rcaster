use std::{fmt, io, time::Duration};

use rcaster_core::ntrip::{ParseError, ParsedHead, parse_head};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    time::timeout,
};

pub const DEFAULT_HEAD_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub enum ReadHeadError {
    Io(io::Error),
    Timeout(Duration),
    UnexpectedEof,
    Parse(ParseError),
}

impl fmt::Display for ReadHeadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "request head read failed: {error}"),
            Self::Timeout(duration) => {
                write!(formatter, "request head timed out after {duration:?}")
            }
            Self::UnexpectedEof => formatter.write_str("connection closed during request head"),
            Self::Parse(error) => write!(formatter, "invalid request head: {error}"),
        }
    }
}

impl std::error::Error for ReadHeadError {}

pub async fn read_request_head<R>(
    reader: &mut R,
    buffered: &mut Vec<u8>,
    limit: Duration,
) -> Result<ParsedHead, ReadHeadError>
where
    R: AsyncRead + Unpin,
{
    timeout(limit, async {
        loop {
            match parse_head(buffered) {
                Ok(parsed) => {
                    buffered.drain(..parsed.consumed);
                    return Ok(parsed);
                }
                Err(ParseError::Incomplete) => {}
                Err(error) => return Err(ReadHeadError::Parse(error)),
            }

            let mut chunk = [0_u8; 4096];
            let read = reader.read(&mut chunk).await.map_err(ReadHeadError::Io)?;
            if read == 0 {
                return Err(ReadHeadError::UnexpectedEof);
            }
            buffered.extend_from_slice(&chunk[..read]);
        }
    })
    .await
    .map_err(|_| ReadHeadError::Timeout(limit))?
}
