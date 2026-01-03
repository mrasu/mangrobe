use std::path::Path;

fn main() {
    let out_dir = Path::new("./src/generated");
    let proto_path = Path::new("../spec/proto/api.proto");
    let prometheus_proto_path = Path::new("./src/prometheus/remote_write_v1.proto");

    println!(
        "cargo:rerun-if-changed=proto/{}",
        proto_path.to_string_lossy()
    );
    println!(
        "cargo:rerun-if-changed=proto/{}",
        prometheus_proto_path.to_string_lossy()
    );

    let proto_dir = proto_path
        .parent()
        .expect("proto file should reside in a directory");

    tonic_prost_build::configure()
        .out_dir(out_dir)
        .compile_protos(&[proto_path], &[proto_dir])
        .unwrap();

    let proto_dir = prometheus_proto_path
        .parent()
        .expect("proto file should reside in a directory");

    tonic_prost_build::configure()
        .out_dir(out_dir)
        .compile_protos(&[prometheus_proto_path], &[proto_dir])
        .unwrap();
}
