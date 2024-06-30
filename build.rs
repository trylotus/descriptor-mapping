use std::{env, path::PathBuf};

fn main() {
    let file_descriptor_set_path = env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("lotus_file_descriptor_set.bin");

    prost_reflect_build::Builder::new()
        .descriptor_pool("crate::lotus::DESCRIPTOR_POOL")
        .file_descriptor_set_path(file_descriptor_set_path)
        .compile_protos(&["proto/lotus.proto"], &["proto"])
        .expect("Failed to compile protos");
}
