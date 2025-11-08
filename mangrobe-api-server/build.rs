use std::env;
use std::path::{Path, PathBuf};

fn main() {
    let proto_path = Path::new("../spec/proto/api.proto");
    let proto_dir = proto_path
        .parent()
        .expect("proto file should reside in a directory");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("api_descriptor.bin"))
        .compile_protos(&[proto_path], &[proto_dir])
        .unwrap()
}
