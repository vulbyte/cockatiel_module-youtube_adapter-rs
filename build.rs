fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../cockatiel_engine-rs/cockatiel_lib/cockatiel_proto";
    println!("cargo:rerun-if-changed={}/youtube_stream_list.proto", proto_dir);

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
