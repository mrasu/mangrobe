use std::path::Path;

fn main() {
    let proto_path = Path::new("../spec/proto/api.proto");
    let proto_dir = proto_path
        .parent()
        .expect("proto file should reside in a directory");

    tonic_prost_build::configure()
        .compile_protos(&[proto_path], &[proto_dir])
        .unwrap()
}
