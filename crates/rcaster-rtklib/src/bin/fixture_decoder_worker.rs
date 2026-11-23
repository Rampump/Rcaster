use std::{
    io::{self, BufRead, Write},
    thread,
    time::Duration,
};

use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct Request {
    version: u32,
    id: u64,
    operation: String,
    payload_hex: Option<String>,
}

fn main() -> io::Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();

    for line in stdin.lock().lines() {
        let line = line?;
        let request: Request = match serde_json::from_str(&line) {
            Ok(request) => request,
            Err(error) => {
                writeln!(
                    stdout,
                    "{}",
                    json!({
                        "version": 1,
                        "id": 0,
                        "result": {"kind": "error", "data": error.to_string()}
                    })
                )?;
                stdout.flush()?;
                continue;
            }
        };
        if request.version != 1 {
            writeln!(
                stdout,
                "{}",
                json!({
                    "version": 1,
                    "id": request.id,
                    "result": {"kind": "error", "data": "unsupported protocol version"}
                })
            )?;
            stdout.flush()?;
            continue;
        }

        match request.operation.as_str() {
            "capability_probe" => {
                writeln!(
                    stdout,
                    "{}",
                    json!({
                        "version": 1,
                        "id": request.id,
                        "result": {
                            "kind": "capability",
                            "data": {
                                "engine": "fixture-decoder",
                                "formats": ["rtcm3", "rtcm2"]
                            }
                        }
                    })
                )?;
            }
            "decode" => {
                let payload = decode_hex(request.payload_hex.as_deref().unwrap_or_default());
                match payload.as_slice() {
                    b"crash" => std::process::exit(7),
                    b"hang" => {
                        thread::sleep(Duration::from_secs(60));
                        continue;
                    }
                    b"large" => {
                        write!(stdout, "{{\"padding\":\"")?;
                        for _ in 0..2 * 1024 * 1024 {
                            write!(stdout, "x")?;
                        }
                        writeln!(stdout, "\"}}")?;
                    }
                    _ => {
                        writeln!(
                            stdout,
                            "{}",
                            json!({
                                "version": 1,
                                "id": request.id,
                                "result": {
                                    "kind": "decoded",
                                    "data": {
                                        "format": "fixture",
                                        "message_numbers": [1005, 1074]
                                    }
                                }
                            })
                        )?;
                    }
                }
            }
            _ => {
                writeln!(
                    stdout,
                    "{}",
                    json!({
                        "version": 1,
                        "id": request.id,
                        "result": {"kind": "error", "data": "unknown operation"}
                    })
                )?;
            }
        }
        stdout.flush()?;
    }
    Ok(())
}

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .filter_map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
        })
        .collect()
}
