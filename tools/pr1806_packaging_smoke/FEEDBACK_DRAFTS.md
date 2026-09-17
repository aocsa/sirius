# PR #1806 — new review feedback drafts

> Local note, not posted. Line numbers and permalinks are for PR head
> [`115329b81a`](https://github.com/sirius-db/sirius/commit/115329b81a338263824230a488c75e647ac33471)
> (`fix(build): export installed header requirements`) on
> [sirius-db/sirius#1806](https://github.com/sirius-db/sirius/pull/1806).
>
> These three items are **new**. They do not repeat the resolved threads on
> `COMPONENT sirius_library`, `${CMAKE_INSTALL_INCLUDEDIR}`, keeping
> `sirius_extension.hpp` internal, legacy include dir, CODEOWNERS, Simpatico
> include widening, `sirius_ffi.h` in the Rust README, `bootstrap-template.py`,
> or `PUBLIC cxx_std_20`.
>
> Reproduced locally with [PR_digests/pr1806_smoke](pr1806_smoke/README.md)
> (`./run.sh`, results in [pr1806_smoke/RESULTS.md](pr1806_smoke/RESULTS.md)).
> F1 and F3 failed as claimed. F2 is a coverage gap: the prefix works when used,
> but a source-tree consumer still succeeds after the prefix is deleted.
>
> Voice: GitHub review English, short, no em dashes.

---

## Suggestion 1 — Do not install `exception.hpp` as a supported library header

**Ask:** keep `sirius/exception.hpp` on the engine include path, but stop shipping
it as part of the installed public tree.

**Why:** #1751 asked for a small installed API with minimal dependencies. The
embedding header never includes the exception types. Installing the whole
`include/sirius/` directory still publishes `sirius::internal_exception` (and
friends) as package ABI. Those types are used from operators and CUDA TUs, not
from `sirius::ffi::Context`.

### Code ranges

| What | Path | Lines | Permalink |
|---|---|---|---|
| Installs every file under `include/sirius/` | [`CMakeLists.txt`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L789-L792) | 789–792 | [L789-L792](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L789-L792) |
| Public include root (build + install) | [`CMakeLists.txt`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L620-L623) | 620–623 | [L620-L623](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L620-L623) |
| Shared library publishes the same include dir | [`CMakeLists.txt`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L724-L726) | 724–726 | [L724-L726](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L724-L726) |
| Embedding API (stdlib only, no `exception.hpp`) | [`include/sirius/ffi.hpp`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/include/sirius/ffi.hpp#L17-L33) | 17–33 | [L17-L33](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/include/sirius/ffi.hpp#L17-L33) |
| Same file: no include of `exception.hpp` through EOF | [`include/sirius/ffi.hpp`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/include/sirius/ffi.hpp#L171-L185) | 171–185 | [L171-L185](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/include/sirius/ffi.hpp#L171-L185) |
| Installed exception types (`std::format_string`) | [`include/sirius/exception.hpp`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/include/sirius/exception.hpp#L17-L34) | 17–34 | [L17-L34](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/include/sirius/exception.hpp#L17-L34) |

```cmake
# CMakeLists.txt:789-792
install(
  DIRECTORY include/sirius
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}"
  COMPONENT sirius_library)
```

```cpp
// include/sirius/ffi.hpp:17-33 — public surface, no exception.hpp
 * Public C++ surface for embedding Sirius (FFI use cases, e.g. the Rust
 * `sirius-sys` crate). Intentionally lightweight — a small RAII wrapper that
 * forward-declares the heavy internal type — so consumers bind it without
 * pulling in sirius_context.hpp (and its cudf/rmm/duckdb includes).
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
```

### Draft (inline on `CMakeLists.txt` L789)

```markdown
The `DIRECTORY include/sirius` install publishes `exception.hpp` next to `ffi.hpp`, but `ffi.hpp` never includes it. `sirius::internal_exception` is engine-wide error plumbing, not the embedding API from #1751.

Once it is in the prefix, it is package ABI (`std::format_string` and all). Keep the file on the build include path so in-tree TUs still compile, and install only `ffi.hpp` (`FILES_MATCHING PATTERN "ffi.hpp"`, or move the exceptions under `src/`).
```

---

## Suggestion 2 — The first-party consumer never uses the installed package

**Ask:** compile `sirius-sys` (or a tiny CMake smoke target) against the installed
prefix, not only the repo `include/` tree and the CMake build directory.

**Why:** this PR now exports `sirius::sirius` and installs headers. The only
in-tree consumer still hardcodes `repo.join("include")` and looks for
`libsirius.so` / `sirius.duckdb_extension` under `$SIRIUS_BUILD_DIR`. A
source-tree build can stay green while `find_package(sirius)` +
`#include <sirius/ffi.hpp>` is broken.

### Code ranges

| What | Path | Lines | Permalink |
|---|---|---|---|
| cxx glue include is always the source tree | [`rust/crates/sirius-sys/build.rs`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/rust/crates/sirius-sys/build.rs#L24-L40) | 24–40 | [L24-L40](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/rust/crates/sirius-sys/build.rs#L24-L40) |
| Link search is the CMake build tree, with a DuckDB-extension symlink stopgap | [`rust/crates/sirius-sys/build.rs`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/rust/crates/sirius-sys/build.rs#L69-L90) | 69–90 | [L69-L90](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/rust/crates/sirius-sys/build.rs#L69-L90) |
| Comment: dedicated `libsirius` does not exist yet | [`rust/crates/sirius-sys/build.rs`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/rust/crates/sirius-sys/build.rs#L17-L20) | 17–20 | [L17-L20](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/rust/crates/sirius-sys/build.rs#L17-L20) |
| Exported CMake target that `build.rs` never finds | [`CMakeLists.txt`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L711-L727) | 711–727 | [L711-L727](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L711-L727) |
| Package files installed under `lib/cmake/sirius` | [`CMakeLists.txt`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L803-L819) | 803–819 | [L803-L819](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L803-L819) |

```rust
// rust/crates/sirius-sys/build.rs:34-40
let ffi_header = repo.join("include/sirius/ffi.hpp");
cxx_build::bridge("src/lib.rs")
    .std("c++20")
    .include(repo.join("include"))
    .compile("sirius_sys");
```

```rust
// rust/crates/sirius-sys/build.rs:69-75
fn resolve_lib_dir(build_dir: &Path, static_link: bool) -> PathBuf {
    let candidates = [build_dir.join("extension/sirius"), build_dir.to_path_buf()];
    let file = if static_link {
        "libsirius.a"
    } else {
        "libsirius.so"
    };
```

### Draft (inline on `rust/crates/sirius-sys/build.rs` L39)

```markdown
`build.rs` still compiles against `repo/include` and links `$SIRIUS_BUILD_DIR`. That never exercises `find_package(sirius)` or the installed `include/sirius/ffi.hpp` this PR now exports.

A source-tree cxx build can pass while a prefix install is unusable. Either teach `sirius-sys` an installed-prefix path, or add a one-file CMake consumer that does `find_package(sirius)` and `#include <sirius/ffi.hpp>`. The in-tree symlink stopgap can stay for developer builds.
```

---

## Suggestion 3 — CMake package name and install component are not the same thing

**Ask:** make `check_required_components` match a real package component, or drop
it until one exists.

**Why:** install rules use `COMPONENT sirius_library`. The generated config calls
`check_required_components(sirius)`, which looks for `sirius_<component>_FOUND`.
Nothing sets `sirius_sirius_library_FOUND`. `find_package(sirius REQUIRED)` still
works. `find_package(sirius REQUIRED COMPONENTS sirius_library)` does not, even
though that is the name on every `install(... COMPONENT ...)` in this block.

### Code ranges

| What | Path | Lines | Permalink |
|---|---|---|---|
| Generated package config | [`cmake/sirius-config.cmake.in`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/cmake/sirius-config.cmake.in#L1-L5) | 1–5 | [L1-L5](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/cmake/sirius-config.cmake.in#L1-L5) |
| Header install component `sirius_library` | [`CMakeLists.txt`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L789-L792) | 789–792 | [L789-L792](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L789-L792) |
| `configure_package_config_file` (no `COMPONENTS`) | [`CMakeLists.txt`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L794-L801) | 794–801 | [L794-L801](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L794-L801) |
| Target / export / config install, same component | [`CMakeLists.txt`](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L803-L819) | 803–819 | [L803-L819](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L803-L819) |

```cmake
# cmake/sirius-config.cmake.in:1-5
@PACKAGE_INIT@

include("${CMAKE_CURRENT_LIST_DIR}/sirius-targets.cmake")

check_required_components(sirius)
```

```cmake
# CMakeLists.txt:803-819
install(
  TARGETS sirius_shared
  EXPORT sirius-targets
  LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}" COMPONENT sirius_library
  ...
)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/sirius-config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/sirius-config-version.cmake"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/sirius"
  COMPONENT sirius_library)
```

### Draft (inline on `cmake/sirius-config.cmake.in` L5)

```markdown
`check_required_components(sirius)` looks for `sirius_<component>_FOUND`. The install rules use `COMPONENT sirius_library`, and nothing in this file sets `sirius_sirius_library_FOUND`.

`find_package(sirius REQUIRED)` is fine. `find_package(sirius REQUIRED COMPONENTS sirius_library)` is not, even though that is the name on the install rules. Either set the `*_FOUND` flag here so the CMake component matches the install component, or drop `check_required_components` until there is a real package component.
```

---

## General PR comment (conversation tab)

Use this as a single top-level review comment if you do not want three inline
notes. It names the same three asks and points at the same ranges.

```markdown
The header split and the latest install-interface / `cxx_std_20` fix look right, and the earlier install-component threads look closed. Three leftover packaging gaps, none of them the resolved `COMPONENT` / `CMAKE_INSTALL_INCLUDEDIR` / extension-header items:

1. `install(DIRECTORY include/sirius)` at [CMakeLists.txt L789-L792](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L789-L792) ships `exception.hpp` as public ABI, but [ffi.hpp L17-L33](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/include/sirius/ffi.hpp#L17-L33) never includes it. Install only `ffi.hpp`, or move the exceptions under `src/`.

2. [sirius-sys/build.rs L24-L40](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/rust/crates/sirius-sys/build.rs#L24-L40) still compiles against `repo/include` and links `$SIRIUS_BUILD_DIR`. Nothing in-tree calls `find_package(sirius)` on the prefix this PR exports. A source-tree cxx build can pass while the installed package is unusable.

3. [sirius-config.cmake.in L1-L5](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/cmake/sirius-config.cmake.in#L1-L5) runs `check_required_components(sirius)` while every install rule uses `COMPONENT sirius_library` ([CMakeLists.txt L803-L819](https://github.com/sirius-db/sirius/blob/115329b81a338263824230a488c75e647ac33471/CMakeLists.txt#L803-L819)). Nothing sets `sirius_sirius_library_FOUND`, so `find_package(sirius REQUIRED COMPONENTS sirius_library)` fails.

Happy to take these as follow-ups if you want this split to land first.
```

---

## How to post

Not posted. From the repo:

```bash
# conversation-tab comment (general draft)
gh pr comment 1806 --repo sirius-db/sirius --body-file - <<'EOF'
...paste general draft...
EOF
```

Inline comments need `gh api` on `pulls/1806/comments` with `commit_id=115329b81a338263824230a488c75e647ac33471` and a `path` + `line` from the tables above.

Suggested anchors if posting inline:

| Suggestion | `path` | `line` (RIGHT) |
|---|---|---|
| 1 | `CMakeLists.txt` | 789 |
| 2 | `rust/crates/sirius-sys/build.rs` | 39 |
| 3 | `cmake/sirius-config.cmake.in` | 5 |
