#[cfg(feature = "rtklib-ffi")]
mod worker {
    use std::{
        ffi::{CStr, c_char, c_int, c_void},
        io::{self, BufRead, Write},
    };

    use serde::Deserialize;
    use serde_json::json;

    use rcaster_rtklib as _;

    unsafe extern "C" {
        fn rcaster_rtklib_new() -> *mut c_void;
        fn rcaster_rtklib_free(instance: *mut c_void);
        fn rcaster_rtklib_version() -> *const c_char;
        fn rcaster_rtklib_decode_rtcm3(
            instance: *mut c_void,
            data: *const u8,
            length: usize,
            types: *mut c_int,
            max_types: c_int,
        ) -> c_int;
    }

    const RTCM3_1005_PROBE: &str = "D300133ED0000236FDB80DDE08005B2BC108A7B98D3DD8AB37";

    #[derive(Deserialize)]
    struct Request {
        version: u32,
        id: u64,
        operation: String,
        payload_hex: Option<String>,
    }

    pub(super) fn run() -> io::Result<()> {
        let instance = Instance::new().map_err(io::Error::other)?;
        let version = instance.version().to_owned();
        let probe = instance.decode(&decode_hex(RTCM3_1005_PROBE));
        let capability_formats = if probe.as_ref().is_ok_and(|types| !types.is_empty()) {
            vec!["rtcm3"]
        } else {
            Vec::new()
        };
        let stdin = io::stdin();
        let mut stdout = io::stdout().lock();

        for line in stdin.lock().lines() {
            let line = line?;
            let request: Request = match serde_json::from_str(&line) {
                Ok(request) => request,
                Err(error) => {
                    write_response(&mut stdout, 0, "error", error.to_string())?;
                    continue;
                }
            };
            if request.version != 1 {
                write_response(
                    &mut stdout,
                    request.id,
                    "error",
                    "unsupported protocol version",
                )?;
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
                                    "engine": format!("RTKLIB {version}"),
                                    "formats": capability_formats
                                }
                            }
                        })
                    )?;
                }
                "decode" => {
                    let payload = decode_hex(request.payload_hex.as_deref().unwrap_or_default());
                    match instance.decode(&payload) {
                        Ok(types) => {
                            writeln!(
                                stdout,
                                "{}",
                                json!({
                                    "version": 1,
                                    "id": request.id,
                                    "result": {
                                        "kind": "decoded",
                                        "data": {
                                            "format": "rtcm3",
                                            "message_numbers": types
                                        }
                                    }
                                })
                            )?;
                        }
                        Err(error) => {
                            write_response(&mut stdout, request.id, "error", error)?;
                        }
                    }
                }
                _ => write_response(&mut stdout, request.id, "error", "unknown operation")?,
            }
            stdout.flush()?;
        }
        Ok(())
    }

    struct Instance {
        inner: *mut c_void,
    }

    impl Instance {
        fn new() -> Result<Self, String> {
            // SAFETY: the shim returns either null or an owned opaque RTKLIB context.
            let inner = unsafe { rcaster_rtklib_new() };
            if inner.is_null() {
                return Err("RTKLIB initialization failed".to_owned());
            }
            Ok(Self { inner })
        }

        fn version(&self) -> &str {
            // SAFETY: RTKLIB returns a static version string for the process lifetime.
            unsafe { CStr::from_ptr(rcaster_rtklib_version()) }
                .to_str()
                .unwrap_or("unknown")
        }

        fn decode(&self, payload: &[u8]) -> Result<Vec<u16>, String> {
            let mut types = [0_i32; 64];
            // SAFETY: the instance is non-null, payload is valid for its length, and
            // RTKLIB only writes through `types` up to `types.len()`.
            let count = unsafe {
                rcaster_rtklib_decode_rtcm3(
                    self.inner,
                    payload.as_ptr(),
                    payload.len(),
                    types.as_mut_ptr(),
                    types.len() as c_int,
                )
            };
            if count < 0 {
                return Err(format!("RTKLIB decode failed with status {count}"));
            }
            Ok(types[..(count as usize).min(types.len())]
                .iter()
                .map(|value| *value as u16)
                .collect())
        }
    }

    impl Drop for Instance {
        fn drop(&mut self) {
            // SAFETY: `inner` was allocated by rcaster_rtklib_new and is freed once.
            unsafe { rcaster_rtklib_free(self.inner) };
        }
    }

    fn write_response(
        stdout: &mut impl Write,
        id: u64,
        kind: &str,
        message: impl Into<String>,
    ) -> io::Result<()> {
        writeln!(
            stdout,
            "{}",
            json!({
                "version": 1,
                "id": id,
                "result": {"kind": kind, "data": message.into()}
            })
        )?;
        stdout.flush()
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
}

#[cfg(feature = "rtklib-ffi")]
fn main() -> std::io::Result<()> {
    worker::run()
}

#[cfg(not(feature = "rtklib-ffi"))]
fn main() {
    eprintln!("rtklib-decoder-worker requires the rtklib-ffi feature");
    std::process::exit(1);
}
