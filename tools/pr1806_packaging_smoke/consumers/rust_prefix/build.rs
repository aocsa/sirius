//! What a first-party consumer would look like if it used the installed
//! package instead of the source tree (sirius-sys does not do this today).
use std::env;
use std::path::{Path, PathBuf};

fn main() {
    let prefix = PathBuf::from(env::var("SIRIUS_PREFIX").expect("SIRIUS_PREFIX"));
    let include_dir = first_existing(&[
        prefix.join("include"),
        prefix.join("usr/include"),
    ]);
    let lib_dir = first_existing(&[
        prefix.join("lib"),
        prefix.join("usr/lib"),
        prefix.join("lib64"),
        prefix.join("usr/lib64"),
    ]);

    cc::Build::new()
        .cpp(true)
        .std("c++20")
        .include(&include_dir)
        .file("src/probe.cpp")
        .compile("sirius_prefix_smoke");

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=sirius");
    println!("cargo:rerun-if-env-changed=SIRIUS_PREFIX");
}

fn first_existing(cands: &[PathBuf]) -> PathBuf {
    cands
        .iter()
        .find(|p| p.is_dir())
        .cloned()
        .unwrap_or_else(|| panic!("none of {:?} exist", cands))
}

#[allow(dead_code)]
fn _keep_path_import(_: &Path) {}
