fn main() {
    println!("cargo:rerun-if-changed=../../proto/admission_coordinator.proto");
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(
            &["../../proto/admission_coordinator.proto"],
            &["../../proto"],
        )
        .expect("compile admission coordinator protobuf");
}
