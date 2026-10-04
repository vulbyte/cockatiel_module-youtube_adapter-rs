fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Vendored copy of the YouTube gRPC live-chat API proto — this module is
    // self-contained and does not reach into the engine's repo.
    let proto_dir = ".";
    println!("cargo:rerun-if-changed={}/youtube_stream_list.proto", proto_dir);

    // Point protoc at the vendored binary so consumers don't need a system
    // `protoc` install (tonic-build shells out to protoc on PATH otherwise).
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_build::configure()
        .build_server(false)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .field_attribute(
            "youtube.api.v3.LiveChatGiftDetails.gift_duration",
            "#[serde(skip)]",
        )
        .compile_protos(&[format!("{}/youtube_stream_list.proto", proto_dir)], &[proto_dir])?;

    Ok(())
}
