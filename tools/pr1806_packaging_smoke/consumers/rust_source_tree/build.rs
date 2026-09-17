//! Mirrors rust/crates/sirius-sys/build.rs include + link discovery:
//! source-tree `include/` and `$SIRIUS_BUILD_DIR`, never the install prefix.
use std::env;
use std::path::{Path, PathBuf};

fn main() {
    let src = PathBuf::from(env::var("SIRIUS_SRC").expect("SIRIUS_SRC"));
    let build_dir = PathBuf::from(env::var("SIRIUS_BUILD_DIR").expect("SIRIUS_BUILD_DIR"));
    let ffi_header = src.join("include/sirius/ffi.hpp");

    cc::Build::new()
        .cpp(true)
        .std("c++20")
        .include(src.join("include"))
        .file("src/probe.cpp")
        .compile("sirius_source_tree_smoke");

    let lib_dir = resolve_lib_dir(&build_dir);
    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=sirius");
    println!("cargo:rerun-if-changed={}", ffi_header.display());
}

fn resolve_lib_dir(build_dir: &Path) -> PathBuf {
    let candidates = [
        build_dir.to_path_buf(),
        build_dir.join("extension/sirius"),
    ];
    for dir in &candidates {
        for name in ["libsirius.dylib", "libsirius.so", "sirius.dll"] {
            if dir.join(name).is_file() {
                return dir.clone();
            }
        }
    }
    panic!(
        "no libsirius in {} or {}/extension/sirius (source-tree smoke expects the CMake build dir, not a prefix)",
        build_dir.display(),
        build_dir.display()
    );
}
