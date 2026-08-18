# Building Sirius + StarRocks with NIXL, from scratch

End-to-end build of the Sirius-as-StarRocks-compute-node demo on a fresh host: the Sirius GPU
engine, the Rust compute node linked against **nixl**, and the StarRocks front end.

Verified 2026-08-11 on `presto-gb200-gcn-18` (4× GB200, aarch64 Grace, Ubuntu 24.04, CUDA 13.0,
gcc 13.3.0, OpenJDK 21). Anything marked **VERIFIED** was checked on that box while writing this.

> **If nixl is missing or mislinked, nothing errors.** `nixl-sys` falls back to a dlopen stub and
> the cluster comes up and answers queries — over a host bounce, ~200× slower than NVLink. §2 and
> §6 exist to make that failure loud. Do not skip them.

---

## 0. Layout and the one external input

```
<parent>/
├── sirius/          the repo
└── tools/           nixl + UCX installs, OUTSIDE the source tree
    ├── nvda_nixl/
    └── ucx-install/
```

`scripts/cn-env.sh` derives every machine-specific path from **its own location**, so the repo
works from any clone path. The only input is `TOOLS_DIR`, defaulting to a `tools/` directory
**next to the repo root**. Override it (or `NIXL_PREFIX` / `UCX_PREFIX` / `NIXL_PLUGIN_DIR`
individually) if your installs live elsewhere — an already-exported value always wins.

```bash
export REPO=$HOME/aocsa/sirius          # adjust
export TOOLS=$(dirname $REPO)/tools
```

### Prerequisites

| | Needed for |
|---|---|
| NVIDIA driver + CUDA 12 or 13 | engine; `pixi.toml` has `cuda12`/`cuda13` feature envs |
| `pixi >= 0.71` | all builds (`requires-pixi` in `pixi.toml`) |
| system `gcc`/`g++` **and** system `ld` in `/usr/bin` | the nixl-sys build — see §5 |
| OpenJDK 17+ and Maven | **only** if you must build the FE from source (§4) |
| `sudo` | only for the apt build-deps in §2 |

---

## 1. Clone

```bash
git clone <sirius-remote> "$REPO"
cd "$REPO"
git submodule update --init --recursive
```

Submodules (`.gitmodules`, **VERIFIED**): `duckdb`, `substrait`, `cucascade`, `duckdb-python`,
`vcpkg`, `.claude/claude-tools`, `experimental/starrocks/starrocks`, `experimental/starrocks/brpc`.

> **Worktrees do not auto-init submodules.** After `git worktree add`, run
> `git submodule update --init --recursive` again or the build fails on missing headers.

---

## 2. Build NIXL and UCX

The CN's cross-node exchange rides **nixl → UCX → `cuda_ipc` → NVLink**. Build UCX first; nixl's
UCX plugin links it.

```bash
mkdir -p "$TOOLS" && cd "$TOOLS"
export CUDA_HOME=${CUDA_HOME:-/usr/local/cuda}

sudo apt-get update
sudo apt-get install -y meson ninja-build pkg-config \
                        libhwloc-dev libnuma-dev pybind11-dev libcufile-dev
```

> Ubuntu's distro libfabric is often older than nixl needs (≥ 1.21, and `fi_ext.h`). If
> `pkg-config --modversion libfabric` reports < 1.21, remove it so meson skips that plugin:
> `sudo apt-get remove -y libfabric-dev libfabric-bin libfabric1`

### UCX — `--enable-mt` is mandatory

```bash
wget -nc https://github.com/openucx/ucx/releases/download/v1.21.0/ucx-1.21.0.tar.gz
tar xf ucx-1.21.0.tar.gz && cd ucx-1.21.0
./configure --prefix="$TOOLS/ucx-install" --with-cuda="$CUDA_HOME" --enable-mt
make -j"$(nproc)" install
cd "$TOOLS"
```

`--enable-mt` is not optional — the nixl agent is touched from a dedicated thread.
**VERIFIED on this box:** UCX 1.21.0 at `$TOOLS/ucx-install`.

### GDS staging prefix (only if you want the cuFile plugins)

Meson's default `gds_path` is `/usr/local/cuda`, but `libcufile-dev` installs headers under
`/usr/include`, so stage a tiny prefix. Meson also rejects include paths that traverse the source
tree via `..` — always pass realpath'd absolutes.

```bash
ARCH=$(uname -m)
mkdir -p "$TOOLS/gds-install/include" "$TOOLS/gds-install/lib64"
ln -sfn /usr/include/cufile.h                       "$TOOLS/gds-install/include/cufile.h"
ln -sfn /usr/lib/$ARCH-linux-gnu/libcufile.so       "$TOOLS/gds-install/lib64/libcufile.so"
ln -sfn /usr/lib/$ARCH-linux-gnu/libcufile.so.0     "$TOOLS/gds-install/lib64/libcufile.so.0"
```

The exchange path needs only the **UCX** plugin; GDS is for GPUDirect Storage and is optional.

### nixl

```bash
git clone https://github.com/ai-dynamo/nixl.git "$TOOLS/nixl-src"
cd "$TOOLS/nixl-src"
meson setup build --prefix="$TOOLS/nvda_nixl" \
      -Ducx_path="$(realpath "$TOOLS/ucx-install")" \
      -Dgds_path="$(realpath "$TOOLS/gds-install")"
ninja -C build install
```

**Gate — a missing UCX plugin is the single most common bring-up failure:**

```bash
ls "$TOOLS"/nvda_nixl/lib/*-linux-gnu/plugins/
```

Must contain `libplugin_UCX.so`. **VERIFIED on this box** it prints `libplugin_UCX.so`,
`libplugin_GDS.so`, `libplugin_GDS_MT.so`, `libplugin_POSIX.so` and two Prometheus exporters.

---

## 3. Build the Sirius engine

```bash
cd "$REPO"
pixi run make                 # long: CUDA + cuDF
```

Produces `build/release/extension/sirius/sirius.duckdb_extension` and the `build/release/duckdb`
binary with the extension statically linked. Add `-e cuda12` on a CUDA 12 host
(`pixi run -e cuda12 make`); the default env targets CUDA 13.

After a failed build, wipe before retrying — a half-populated `build/` produces confusing errors:

```bash
pixi run make clean && pixi run make
```

---

## 4. The StarRocks front end

The FE is normally shipped **pre-packaged**, so the multi-hour Maven build is not needed:

```bash
cd "$REPO/experimental/starrocks"
pixi run fe-check             # asserts starrocks/output/fe/bin/start_fe.sh exists
```

`fe-check` depends on `apply-starrocks-patches`, so it also applies §4.1's patch. If the package
is missing, either copy `starrocks/output/fe` from a box that has it, or build it:

```bash
pixi run fe-build             # long — full Maven build of the StarRocks FE
```

### 4.1 The StarRocks proto patch — read this before `git status` confuses you

The Sirius-only exchange RPCs (`exchange_nixl_md`, `request_staging_lease`, `transmit_packed`)
live in `patches/nixl-exchange-proto.patch` (**VERIFIED**: one patch file) and are applied onto the
stock submodule by `scripts/apply-starrocks-patches.sh`. They are **never committed into the
submodule**, so the gitlink always points at an upstream commit.

**Both builds consume the patched proto** — the FE's Maven build generates Java stubs from it, and
the CN's `build.rs` runs prost over the same `gensrc/proto/internal_service.proto`. That is why
`cn-build`, `cn-test`, `cn-run` and `fe-check` all depend on `apply-starrocks-patches`. The script
is idempotent (it detects an applied patch via `git apply --reverse --check` and skips it), so a
`git submodule update` that reverts to stock is healed by the next build.

**Consequence:** the superproject permanently shows `Subproject commit <hash>-dirty`. That is the
applied patch sitting as an uncommitted working-tree change, **by design**. Do not "fix" it by
committing inside the submodule. `git add experimental/starrocks/starrocks` records only the
commit hash, never the dirt. To silence it locally:
`git config diff.ignoreSubmodules dirty`.

---

## 5. Build the compute node (engine + nixl linked)

```bash
cd "$REPO/experimental/starrocks"
pixi run cn-build
```

`cn-build` depends on `engine-build` and `apply-starrocks-patches`, so a cold tree builds
libsirius first (slow); a warm tree finishes in seconds. It sources `scripts/cn-env.sh`, which
derives everything and sets `NIXL_NO_STUBS_FALLBACK=1`.

### What `cn-env.sh` works around, and why each line exists

Every one of these was a real build failure. They are documented in the script itself; repeated
here so you recognise the symptom.

| Setting | Symptom if missing |
|---|---|
| `NIXL_NO_STUBS_FALLBACK=1` | **No error at all.** `nixl-sys` compiles a dlopen stub; you find out at runtime as an agent-creation error, or as a mysteriously slow transport |
| `unset CXXFLAGS CFLAGS CPPFLAGS CPATH …`, `CC=/usr/bin/gcc` | pixi activation injects flags mixing `/usr/include` with the conda sysroot → nixl-sys fails on `bits/timesize.h` |
| `PATH="/usr/bin:$PATH"` | gcc resolves `ld` from PATH; under `pixi run` that is conda's ld, which links the conda sysroot's `libpthread.so.0` against the system libc → **39 `GLIBC_PRIVATE` undefined references** |
| `LIBRARY_PATH=…:/usr/lib/<arch>-linux-gnu:…` | `libsirius.so` declares `libcuda.so.1`/`libnvidia-ml.so.1` in `DT_NEEDED`, so the link must *find* them → `undefined reference to 'cuLaunchKernel'`, `'nvmlDeviceGetMemoryAffinity'` |
| `LIBCLANG_PATH` + `BINDGEN_EXTRA_CLANG_ARGS` | nixl-sys bindgen needs libclang **and its builtin headers**; the system libclang ships none, the repo pixi env's clang does |
| `UCX_TLS=cuda_copy,cuda_ipc,tcp,self` | without `cuda_copy` UCX cannot detect VRAM pointers; without `cuda_ipc` same-host GPU→GPU takes a host bounce |

> **Do not** try to fix the linker with `RUSTFLAGS="-C link-arg=-B/usr/bin"`. Setting `RUSTFLAGS`
> invalidates cargo's entire fingerprint cache, which re-runs the nixl-sys build script, which then
> fails on `bits/timesize.h` — a separate latent breakage a warm `target/` had been hiding.
> Prepending `PATH` gets the system `ld` without touching the fingerprint.

### Using the env by hand

**Source it, never execute it** — run as a child process it configures a shell that immediately
exits. It also *unsets* `CXXFLAGS`/`CFLAGS`/`CPATH`, so if your shell needs those for other work,
use a subshell:

```bash
( source scripts/cn-env.sh && cargo build --release -p sirius-starrocks-cn )
```

It fails loudly rather than half-configuring: no nixl under `$TOOLS_DIR` tells you to set
`TOOLS_DIR`; no clang headers tells you to run `pixi install` at the repo root.

---

## 6. Prove nixl is actually linked

This is the step that separates a working build from a silently-degraded one.

```bash
readelf -d target/release/sirius-starrocks-cn | grep -Ei 'nixl|sirius'
```

**VERIFIED on this box** — exactly three `NEEDED` entries:

```
(NEEDED)  Shared library: [libnixl.so]
(NEEDED)  Shared library: [libnixl_build.so]
(NEEDED)  Shared library: [sirius.duckdb_extension]
```

**If `libnixl.so` is absent, the stub path was taken.** Recheck `NIXL_NO_STUBS_FALLBACK` and
rebuild. (`ldd` shows the same plus resolved paths, but needs `LD_LIBRARY_PATH` from `cn-env.sh`
or it prints "not found".)

Then confirm the hardware side — every GPU pair must read `NV#`, not `PIX`/`PHB`/`SYS`:

```bash
nvidia-smi topo -m
```

---

## 7. Test

In cost order:

```bash
cd "$REPO/experimental/starrocks"

# Pure Rust, no engine, no GPU — what CI runs
pixi run cn-test-no-engine

# The plan translator
( source scripts/cn-env.sh && cargo test -p starrocks-plan-translator )

# Engine-linked, includes the 76-case wire-type parity gate
pixi run cn-test

# C++ engine tests (only if src/** changed)
cd "$REPO" && pixi run make test
```

---

## 8. Bring up a cluster

```bash
cd "$REPO/experimental/starrocks"
pixi run cluster2 &      # 1 FE + 2 CNs; `cluster` for the 1-CN variant

# One alive CN means it is STILL BOOTING — act only on the full count
until [ "$(mysql -h127.0.0.1 -P9030 -uroot -N -e 'SHOW COMPUTE NODES;' \
           | awk -F'\t' '$9=="true"' | wc -l)" -ge 2 ]; do sleep 5; done
```

For the NUMA-pinned 4-GPU layout use `configs/gb200-4gpu/cluster4-numa.sh`, which additionally
asserts the GPU↔socket mapping and refuses to bind host memory onto a CPU-less GPU-HBM NUMA node.

Teardown — verify **both** lines:

```bash
pkill -f '[s]irius-starrocks-cn'; pkill -f '[S]tarRocksFE'
nvidia-smi --query-gpu=memory.used --format=csv,noheader     # must return to ~0 MiB
```

### Confirm the transport is on the fast path

```bash
grep -hE "P2P enabled|no P2P access" .cn0/log/sirius_*.log
```

Expect `n*(n-1)` `P2P enabled` lines per CN and **zero** `no P2P access`. The fallback to
host-staged copy is silent, which is exactly why this grep belongs in every bring-up.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `no nixl install at …` from `cn-env.sh` | `TOOLS_DIR` wrong or nixl not built | §2, or export `TOOLS_DIR` |
| `no clang builtin headers under …` | repo pixi env not installed | `pixi install` at the repo root |
| nixl-sys: `bits/timesize.h: No such file` | conda `CXXFLAGS` leaked in | source `cn-env.sh`; do not set `RUSTFLAGS` |
| 39 × `GLIBC_PRIVATE` undefined refs | conda `ld` used | `cn-env.sh` prepends `/usr/bin` to `PATH` |
| `undefined reference to 'cuLaunchKernel'` | driver libs not on `LIBRARY_PATH` | source `cn-env.sh` |
| `readelf` shows no `libnixl.so` | stub fallback | set `NIXL_NO_STUBS_FALLBACK=1`, rebuild |
| Cluster runs but transfers are ~200× slow | `cuda_ipc` unavailable | check `UCX_TLS`, `libplugin_UCX.so`, `nvidia-smi topo -m` |
| `StarRocks submodule is not checked out` | submodules not initialised | `git submodule update --init --recursive` |
| Submodule permanently shows `-dirty` | **expected** — the applied proto patch | §4.1; do not commit inside the submodule |
| `pkill` pattern matches nothing | the binary is `sirius-starrocks-cn`; the FE class is `StarRocksFE` | use those exact names |

---

## Quick reference

```bash
# fresh host, end to end
git clone <remote> "$REPO" && cd "$REPO"
git submodule update --init --recursive
# ... §2: build UCX + nixl into $TOOLS ...
pixi run make                                    # engine
cd experimental/starrocks
pixi run fe-check                                # FE (applies the proto patch)
pixi run cn-build                                # CN, engine + nixl linked
readelf -d target/release/sirius-starrocks-cn | grep -i nixl   # THE gate
pixi run cn-test
pixi run cluster2 &
```

## See also

| Document | Covers |
|---|---|
| `benchmarks/nixl-nvlink/notes-setup.md` | The original runbook — port plan, launch flags, TPC-H data generation, transport verification, tuning |
| `configs/gb200-4gpu/engine-a.env` | The 4-GPU NUMA-pinned configuration and why each value is what it is |
| `../../SNMG-PLAN.md` | Multi-GPU-per-CN (2 GPUs per compute node), not yet implemented |
| `../../.claude/CLAUDE.md` | Repo-wide build/test conventions |
