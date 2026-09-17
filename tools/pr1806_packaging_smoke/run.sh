#!/usr/bin/env bash
# Reproduce PR #1806 packaging findings.
#
# Default: tiny CMake replica of the 115329b81a install rules (no CUDA, no Sirius).
# GPU machine: point at a real Sirius install prefix after `pixi run make`.
set -u
set -o pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"
WORK="$ROOT/work"
REPLICA="$ROOT/replica"
LOG="$WORK/results.log"

MODE="replica"
PREFIX=""
SIRIUS_SRC="$REPLICA"
SIRIUS_BUILD=""
KEEP_PREFIX=0

usage() {
  cat <<'EOF'
Usage:
  ./run.sh
      Replica only. No CUDA. No Sirius compile.

  ./run.sh --sirius-prefix PREFIX --sirius-build BUILD_DIR [--sirius-src REPO]
      Use a real Sirius install. Does not compile Sirius.
      PREFIX is CMAKE_INSTALL_PREFIX after cmake --install.
      BUILD_DIR is the CMake build tree (usually build/release).
      REPO defaults to this clone.

  ./run.sh --keep-prefix
      Do not delete PREFIX at the end of finding 2.

This harness never runs pixi or make. Build Sirius on the GPU machine first.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sirius-prefix)
      MODE="sirius"
      PREFIX="$2"
      shift 2
      ;;
    --sirius-build)
      SIRIUS_BUILD="$2"
      shift 2
      ;;
    --sirius-src)
      SIRIUS_SRC="$2"
      shift 2
      ;;
    --keep-prefix)
      KEEP_PREFIX=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$MODE" == "sirius" ]]; then
  if [[ -z "$PREFIX" || -z "$SIRIUS_BUILD" ]]; then
    echo "--sirius-prefix and --sirius-build are both required" >&2
    exit 2
  fi
  SIRIUS_SRC="${SIRIUS_SRC:-$REPO_ROOT}"
else
  PREFIX="$WORK/prefix"
  SIRIUS_BUILD="$WORK/replica-build"
  SIRIUS_SRC="$REPLICA"
fi

rm -rf "$WORK"
mkdir -p "$WORK"
: >"$LOG"

pass=0
fail=0

log() { printf '%s\n' "$*" | tee -a "$LOG"; }

run_step() {
  local title="$1"
  shift
  log ""
  log "== $title =="
  if "$@" >>"$LOG" 2>&1; then
    log "OK"
    return 0
  else
    log "FAILED (exit $?)"
    return 1
  fi
}

expect_ok() {
  local id="$1" title="$2"
  shift 2
  if run_step "$title" "$@"; then
    log "RESULT $id: CONFIRMED-PATH (succeeded as predicted)"
    pass=$((pass + 1))
  else
    log "RESULT $id: UNEXPECTED (this was expected to succeed)"
    fail=$((fail + 1))
  fi
}

expect_fail() {
  local id="$1" title="$2"
  shift 2
  if run_step "$title" "$@"; then
    log "RESULT $id: NOT REPRODUCED (this was expected to fail)"
    fail=$((fail + 1))
  else
    log "RESULT $id: REPRODUCED (failed as predicted)"
    pass=$((pass + 1))
  fi
}

cmake_consumer() {
  local name="$1" src="$2"
  local dest="$WORK/$name"
  rm -rf "$dest"
  cmake -S "$src" -B "$dest" -DCMAKE_PREFIX_PATH="$PREFIX"
  cmake --build "$dest"
}

log "mode $MODE"
log "cmake $(cmake --version | head -1)"
log "cxx  $(c++ --version | head -1)"
log "rustc $(rustc --version 2>/dev/null || echo missing)"
log "prefix $PREFIX"
log "sirius_src $SIRIUS_SRC"
log "sirius_build $SIRIUS_BUILD"

if [[ "$MODE" == "replica" ]]; then
  expect_ok SETUP "configure + install replica" \
    bash -c "cmake -S '$REPLICA' -B '$SIRIUS_BUILD' -DCMAKE_INSTALL_PREFIX='$PREFIX' && cmake --build '$SIRIUS_BUILD' && cmake --install '$SIRIUS_BUILD'"
else
  if [[ ! -d "$PREFIX" ]]; then
    log "RESULT SETUP: UNEXPECTED (prefix $PREFIX does not exist; install Sirius first)"
    fail=$((fail + 1))
    log "summary: $pass predicted outcomes, $fail surprises"
    exit 1
  fi
  log "RESULT SETUP: CONFIRMED-PATH (using existing Sirius prefix)"
  pass=$((pass + 1))
fi

log ""
log "== installed tree (headers + cmake package) =="
if [[ -d "$PREFIX" ]]; then
  ( cd "$PREFIX" && find include lib/cmake -type f 2>/dev/null | sort ) | tee -a "$LOG"
fi

if [[ -f "$PREFIX/include/sirius/exception.hpp" ]]; then
  log "RESULT F1-FILE: REPRODUCED (exception.hpp is in the prefix)"
  pass=$((pass + 1))
else
  log "RESULT F1-FILE: NOT REPRODUCED (exception.hpp missing from prefix)"
  fail=$((fail + 1))
fi

expect_ok F1-FFI "C++ consumer: find_package(sirius) + #include <sirius/ffi.hpp>" \
  cmake_consumer cpp_ffi "$ROOT/consumers/cpp_ffi"

expect_ok F1-EXC "C++ consumer: find_package(sirius) + #include <sirius/exception.hpp>" \
  cmake_consumer cpp_exception "$ROOT/consumers/cpp_exception"

expect_fail F3 "C++ consumer: find_package(sirius REQUIRED COMPONENTS sirius_library)" \
  cmake_consumer cpp_component "$ROOT/consumers/cpp_component"

if command -v cargo >/dev/null 2>&1; then
  expect_ok F2-SRC "Rust consumer: source-tree include + SIRIUS_BUILD_DIR (sirius-sys style)" \
    env SIRIUS_SRC="$SIRIUS_SRC" SIRIUS_BUILD_DIR="$SIRIUS_BUILD" \
      CARGO_TARGET_DIR="$WORK/rust-src-target" \
      cargo test --manifest-path "$ROOT/consumers/rust_source_tree/Cargo.toml"

  expect_ok F2-PRE "Rust consumer: installed prefix include + lib" \
    env SIRIUS_PREFIX="$PREFIX" \
      CARGO_TARGET_DIR="$WORK/rust-pre-target" \
      cargo test --manifest-path "$ROOT/consumers/rust_prefix/Cargo.toml"

  if [[ "$KEEP_PREFIX" -eq 0 ]]; then
    rm -rf "$PREFIX"
    log ""
    log "== deleted install prefix $PREFIX =="

    expect_ok F2-SRC-AFTER "Rust source-tree consumer after prefix delete" \
      env SIRIUS_SRC="$SIRIUS_SRC" SIRIUS_BUILD_DIR="$SIRIUS_BUILD" \
        CARGO_TARGET_DIR="$WORK/rust-src-target2" \
        cargo test --manifest-path "$ROOT/consumers/rust_source_tree/Cargo.toml"

    expect_fail F2-PRE-AFTER "Rust prefix consumer after prefix delete" \
      env SIRIUS_PREFIX="$PREFIX" \
        CARGO_TARGET_DIR="$WORK/rust-pre-target2" \
        cargo test --manifest-path "$ROOT/consumers/rust_prefix/Cargo.toml"
  else
    log "skipped F2 prefix-delete steps (--keep-prefix)"
  fi
else
  log "skipped Rust smokes (cargo not on PATH)"
fi

log ""
log "== summary: $pass predicted outcomes, $fail surprises =="
if [[ "$fail" -ne 0 ]]; then
  exit 1
fi
exit 0
