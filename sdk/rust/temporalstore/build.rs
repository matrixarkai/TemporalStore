use std::env;
use std::path::PathBuf;

fn main() {
    if env::var("CARGO_FEATURE_DIRECT").is_err() {
        return;
    }

    println!("cargo:rerun-if-env-changed=TEMPORALSTORE_LIB_DIR");
    println!("cargo:rerun-if-env-changed=TEMPORALSTORE_LIB_NAME");

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let repo_root = manifest_dir.join("../../..");
    let ubuntu_lib_dir = repo_root.join("output-ubuntu22/release/sdk/lib");
    let default_lib_dir = repo_root.join("output/sdk/lib");
    let default_lib_dir = if ubuntu_lib_dir.exists() {
        ubuntu_lib_dir
    } else {
        default_lib_dir
    };
    let lib_dir =
        env::var("TEMPORALSTORE_LIB_DIR").unwrap_or_else(|_| default_lib_dir.display().to_string());
    let lib_name = env::var("TEMPORALSTORE_LIB_NAME").unwrap_or_else(|_| "bcache2".to_string());

    println!("cargo:rustc-link-search=native={lib_dir}");
    println!("cargo:rustc-link-lib={lib_name}");
}
