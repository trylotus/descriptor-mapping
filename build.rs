fn main() {
    protobuf_codegen::Codegen::new()
        .protoc()
        .protoc_path(&protoc_bin_vendored::protoc_bin_path().unwrap())
        .include("proto")
        .input("proto/lotus.proto")
        .out_dir("src/proto")
        .run_from_script();
}
