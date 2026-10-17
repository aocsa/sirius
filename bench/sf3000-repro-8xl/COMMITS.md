# Exact commits for the 8xl-sf3k reproduction on GB200

Two engines were built from public `sirius-db/sirius` source. Neither has source edits beyond the
listed PR merges. `prepare-engine.sh <integration|historical> <dir>` rebuilds either one and
checks the resulting source tree.

## Integration engine (current main + current PR heads)

Used for the cold, lukewarm and hot runs. Built on branch `integ/spill-s3-8xl-sf3k`
(pushed to `aocsa/sirius`).

| Step | Commit | What |
|---|---|---|
| base | `f32589fb47ff3a1900c7b5c3170a01df104fc4b4` | `main`, 2026-10-16, "fix(bench): time con.execute() only (#2226)" |
| merge | `0997c1f3fe4b15ab3a4f979a922dbd011e9d0932` | tip of `stacked/spill-compression` = #2182 → #2183 → #2204 (12 commits) |
| merge | `ba9cffa4ba578e05f66b923ba643d3258e299f61` | #2155 head, `fix/io-s3-readahead-and-fused-reads` (4 commits beyond main) |
| result | merges `b77c31ef`, `2d8f15cb`; **source tree `1744e2013ca211c4e1b0a1e2005c0e573bbfa87e`** | |

Commits brought in by `0997c1f3` (beyond `f32589fb`):

```
0997c1f3 feat(compression): compress batches spilled to the host and disk tiers        #2204
946b0562 fix(simpatico): rebuild identity leaves as fixed-width columns                #2183
d4ee2d68 feat(simpatico): support DECIMAL128 in the codegen path                       #2183
9ebcf33f fix(simpatico): report stream creation failures as out-of-memory              #2183
25b80b18 fix(simpatico): key the JIT CUfunction cache by context, not by device        #2183
eb575f98 fix(compression): keep dictionary equality pushdown on nullable columns       #2182
46067837 test(compression): round-trip nullable columns through the host and GPU tiers #2182
c0429be9 refactor(compression): drop per-operator null handling made dead by the validity sidecar  #2182
c8716436 fix(compression): address review comments on nullable columns                 #2182
8a0b19e9 fix(compression): refuse a nullable column in the fused scan filter up front  #2182
9bca4cad fix(compression): allocate the validity mask from the leaf allocator          #2182
e4021466 feat(compression): support nullable columns transparently                     #2182
```

Commits brought in by `ba9cffa4` (#2155, beyond `f32589fb`):

```
ba9cffa4 perf(scan): read the parquet files a scan finds cached before the rest
4d83cd03 test(scan_manager): pass the query lifecycle registry in the lazy-cache tests
8cbf44c1 fix(scan): build the default ioctx's prefetching cache on first use
51f3d169 fix(io): fuse S3 GETs, bound readahead and cache memory
```

The benchmark checkout was `ea547903` on that branch (two `bench/` commits on top of `2d8f15cb`),
so `invocation.json` records tree `4c1ec5ce`; the engine sources are identical to `1744e201`.

## Historical engine (the reproducer package's tested tree)

Matches `sirius-spill-reproducer/provenance.json` exactly; `run.py --source-policy historical`
accepts it.

| Step | Commit | What |
|---|---|---|
| base | `38fe9ba22ac23ab556a9d5cd07d78592c3d3c403` | `main`, 2026-10-15, "fix(late-mat): support nightly cudf null mask buffers (#2187)" |
| merge | `10727ae9874c7d89bb9179484c6be459d2ced97b` | #2204 head as tested on g7e (includes the #2182/#2183 stack) |
| merge | `4d83cd03577a0a320427979baf137e488e485f7c` | #2155 head as tested on g7e |
| result | merges `0bd7f700`, `8d215292`; **source tree `68fee8488bdba0a765bc5ae11d62cb1780a5d74d`** (= `provenance.json`) | |

Commits brought in by `10727ae9` (beyond `38fe9ba2`):

```
10727ae9 feat(compression): compress batches spilled to the host and disk tiers
b66e5806 fix(simpatico): rebuild identity leaves as fixed-width columns
26c915fb feat(simpatico): support DECIMAL128 in the codegen path
9f38e187 fix(simpatico): report stream creation failures as out-of-memory
ee80b12f fix(simpatico): key the JIT CUfunction cache by context, not by device
8a0b19e9 fix(compression): refuse a nullable column in the fused scan filter up front
9bca4cad fix(compression): allocate the validity mask from the leaf allocator
e4021466 feat(compression): support nullable columns transparently
```

Commits brought in by `4d83cd03` (beyond `38fe9ba2`): `4d83cd03`, `8cbf44c1`, `51f3d169` (same as
above, minus `ba9cffa4`).

### How the two engines differ

- **#2204 was rewritten after the g7e runs.** `10727ae9` is not an ancestor of `0997c1f3`; the
  integration stack adds four #2182 review commits (`eb575f98`, `46067837`, `c0429be9`,
  `c8716436`) and the rewritten #2183/#2204 commits differ in 8 source files (+173/−70).
- **#2155 gained one commit:** `ba9cffa4` (read cached parquet files first).
- **Base:** `f32589fb` is one day newer than `38fe9ba2`.

## Shared pins (both engines)

| Component | Commit |
|---|---|
| `duckdb`, `sirius-duckdb/duckdb` | `88ecd0a49d7d3d838e002b2fbc0ce91ccf74110e` (v1.5.6-16254) |
| `cucascade` | `bd4aeedbee1201df68bd042374fd9ab6afb5b46d` |
| `substrait` | `922489d3c8c77140b189ad3847554de141378b04` |
| `sirius-duckdb/extension-ci-tools` | `55e9f697b4fb70f357cc1bd4d1893cbf217ee359` |
| `pixi.lock` sha256 | `6ea424ffcd3f354ef493624ed0c5cc08961056b611edd972bfb564fc755df2d7` |

## Build and host

| | This reproduction | Package (g7e.8xlarge) |
|---|---|---|
| GPU | NVIDIA GB200, 189,471 MiB, capped to 88,272,084,664 B | RTX PRO 6000 Blackwell, 97,887 MiB, `usage_limit_fraction: 0.86` |
| `CUDAARCHS` | `100-real` | `120-real` |
| Driver / CUDA | 580.105.08 / nvcc 13.4.92 (pixi) | 595.91.07 / 13.2.51 |
| CPU | 144-core Grace (aarch64), run pinned to cores 0–31 | 32 vCPU |
| RAM | 1.69 TiB; 200 GiB managed host pool | 256 GiB; 200 GiB managed host pool |
| Data | `/scratch/sirius/datasets/tpch_sf3000` (GPFS, tpchgen 2.0.2, decimal128) | S3 `tpch_sf3000_regenerated` (tpchgen-rs `cdcf74de`) |
| Spill | `/scratch/prestouser/aocsa/spill` (GPFS) | instance NVMe |
| Build | `pixi run env CUDAARCHS=100-real CMAKE_BUILD_PARALLEL_LEVEL=64 make -j64`, pixi 0.81.0 | same with `120-real`, `-j20` |

## Reproducer package

`sirius-spill-reproducer.tar.gz`, sha256
`4be7eef7eb8ff18134e3ffa717bc772c8f600c9f2e55b0fdfd38da4f72200212`. Not vendored; `run.sh`
extracts it and applies `reproducer-gb200.patch` (GPU limit in bytes, spill root from
`REPRO_SPILL_ROOT`). Everything else — YAML values, thread counts, schedules, `.timer`
timing, validation against saved g7e outputs — is the package's own.
