use std::{env, path::PathBuf};

fn main() {
    println!("cargo:rerun-if-env-changed=RTKLIB_SOURCE_DIR");
    if env::var_os("CARGO_FEATURE_RTKLIB_FFI").is_none() {
        return;
    }
    let source_root = PathBuf::from(
        env::var("RTKLIB_SOURCE_DIR")
            .expect("RTKLIB_SOURCE_DIR is required with the rtklib-ffi feature"),
    );
    let source = source_root.join("src");
    println!("cargo:rerun-if-changed=csrc/rtklib_shim.c");
    println!("cargo:rerun-if-changed={}", source.display());
    cc::Build::new()
        .include(&source)
        .file("csrc/rtklib_shim.c")
        .file(source.join("rtcm.c"))
        .file(source.join("rtcm2.c"))
        .file(source.join("rtcm3.c"))
        .file(source.join("rtcm3e.c"))
        .file(source.join("rtkcmn.c"))
        .warnings(false)
        .compile("rcaster_rtklib_shim");
    if env::var_os("CARGO_CFG_TARGET_OS").as_deref() == Some(std::ffi::OsStr::new("windows")) {
        println!("cargo:rustc-link-lib=winmm");
    }
}
