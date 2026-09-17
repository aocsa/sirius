# Smoke results

## No-GPU Mac (replica)

CMake 4.4.0, AppleClang 21, rustc 1.97.1. Replica of PR head `115329b81a`.

Harness-only `-Wl,-rpath` so dyld can load `libsirius`. Not part of the PR.

| ID | Claim | Verdict | Evidence |
|---|---|---|---|
| F1 | `install(DIRECTORY include/sirius)` ships `exception.hpp` as public ABI | **Reproduced** | Prefix contains `include/sirius/exception.hpp`. C++ `find_package(sirius)` + `#include <sirius/exception.hpp>` compiled and linked. |
| F1b | Installed `ffi.hpp` is usable | Works (not a bug) | C++ `find_package(sirius)` + `#include <sirius/ffi.hpp>` compiled and linked. |
| F2 | Source-tree consumer does not use the prefix | **Coverage gap** | Rust source-tree smoke still passed after `rm -rf prefix`. Prefix smoke then failed because `include/` was gone. While the prefix existed, the prefix smoke also passed. |
| F3 | `find_package(sirius REQUIRED COMPONENTS sirius_library)` fails | **Reproduced** | CMake found `sirius-config.cmake` but set `sirius_FOUND` to FALSE. |

CMake error for F3:

```
CMake Error at CMakeLists.txt:3 (find_package):
  Found package configuration file:
    .../prefix/lib/cmake/sirius/sirius-config.cmake
  but it set sirius_FOUND to FALSE so package "sirius" is considered to be
  NOT FOUND.
```

## GPU machine (real Sirius)

Not run from this clone. Use `./run.sh --sirius-prefix ...` after `pixi run make`
and `cmake --install`.
