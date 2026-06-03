use std::env;
use std::path::PathBuf;

fn main() {
    if env::var("CARGO_FEATURE_DIRECT").is_err() {
        return;
    }

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let default_lib_dir = manifest_dir.join("../../../output/sdk/lib");
    let lib_dir =
        env::var("TEMPORALSTORE_LIB_DIR").unwrap_or_else(|_| default_lib_dir.display().to_string());
    let lib_name = env::var("TEMPORALSTORE_LIB_NAME").unwrap_or_else(|_| "bcache2".to_string());

    println!("cargo:rustc-link-search=native={lib_dir}");
    println!("cargo:rustc-link-lib={lib_name}");
}
