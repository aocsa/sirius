# PR #1806 packaging smokes

Reproduce three packaging claims from
[sirius-db/sirius#1806](https://github.com/sirius-db/sirius/pull/1806)
at `115329b81a`. This tree does **not** compile Sirius or CUDA.

## What it checks

1. `install(DIRECTORY include/sirius)` ships `exception.hpp` as public ABI.
2. A source-tree consumer (`sirius-sys` style) still succeeds after the prefix is deleted.
3. `find_package(sirius REQUIRED COMPONENTS sirius_library)` fails because
   `check_required_components(sirius)` never sets `sirius_sirius_library_FOUND`.

## Replica (no GPU)

Needs CMake 3.30+, a C++20 compiler, and optionally Rust/cargo.

```bash
cd tools/pr1806_packaging_smoke
./run.sh
```

This builds the tiny library in `replica/` with the same install/export rules as
the PR, then runs the C++ and Rust consumers. It never calls `pixi` or `make`.

## Real Sirius install (NVIDIA GPU machine)

Build and install Sirius first. This harness still does not compile it.

```bash
# in the clone, on the GPU machine
git submodule update --init --recursive
pixi run make

cmake --install build/release \
  --prefix "$PWD/tools/pr1806_packaging_smoke/work/sirius-prefix" \
  --component sirius_library

cd tools/pr1806_packaging_smoke
./run.sh \
  --sirius-prefix "$PWD/work/sirius-prefix" \
  --sirius-build "$PWD/../../build/release" \
  --sirius-src "$PWD/../.." \
  --keep-prefix
```

`--keep-prefix` skips deleting the install tree at the end of finding 2.

If `cmake --install` is not wired for your preset, any prefix that contains
`include/sirius/ffi.hpp` and `lib/cmake/sirius/sirius-config.cmake` is enough.

## Layout

| Path | Role |
|---|---|
| `replica/` | Standalone CMake package with PR 1806 install rules |
| `consumers/cpp_ffi` | `find_package(sirius)` + `#include <sirius/ffi.hpp>` |
| `consumers/cpp_exception` | same, but `#include <sirius/exception.hpp>` |
| `consumers/cpp_component` | `find_package(sirius REQUIRED COMPONENTS sirius_library)` |
| `consumers/rust_source_tree` | repo `include/` + `$SIRIUS_BUILD_DIR` |
| `consumers/rust_prefix` | `$SIRIUS_PREFIX/include` + `$SIRIUS_PREFIX/lib` |

`work/` is local output and is gitignored.

## Results from a no-GPU Mac

See [RESULTS.md](RESULTS.md). F1 and F3 reproduced. F2 is a coverage gap: the
prefix worked when used, but the source-tree consumer still worked after the
prefix was deleted.
