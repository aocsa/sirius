# sirius-dfd

Writes the `exchange_directives` file of a query for distributed Sirius from DataFusion
Distributed's plan of it. DataFusion plans the query, DataFusion Distributed (DFD) cuts the plan
into stages, and this crate reads the cut back as directive lines. Nothing runs: no DFD worker and
no Arrow Flight (DFD is built without its `grpc` feature). Sirius still plans the query itself; it
follows the lines that name a join or a grouped merge of its own plan, and only where the answer
cannot change (see "Exchange directives" in `docs/super-sirius/distributed-sirius.md`).

The crate is its own Cargo workspace, so the engine's Rust build does not compile DataFusion.

```bash
cargo build --release --manifest-path rust/crates/sirius-dfd/Cargo.toml
target/release/sirius-dfd --data /data/tpch_sf1000 --ranks 4 -o q03.directives q03.sql
```

```text
# q03.sql
# sirius-dfd: ranks=4 target_partitions=16 joins=sirius
MERGE_GROUP_BY l_orderkey,o_orderdate,o_shippriority none  # stage 5, SinglePartitioned
```

Then, on every rank, `exchange_directives: /path/q03.directives` under `operator_params` in the
Sirius configuration (or `SET exchange_directives = '...'` with `SIRIUS_ENABLE_TEST_OPTIONS=1`).

| Option | Meaning |
|---|---|
| `--data DIR` | every subdirectory of `DIR` is a Parquet table of that name |
| `--table NAME=DIR` | one table; repeatable |
| `--ranks N` | the number of Sirius ranks, DFD's worker count |
| `--target-partitions P` | DataFusion's partitions per task, default 16. A table smaller than 16 MiB × P is read by one task |
| `--joins` | also write DFD's choice for each join (`hash`, `broadcast`, `hash width=1`). Without it Sirius keeps its own broadcast rule |
| `--set KEY=VALUE` | a DataFusion or DFD setting, for example `datafusion.optimizer.hash_join_single_partition_threshold=1073741824` |
| `--stages` | print DFD's staged plan to stderr |

What becomes a line:

| DFD's plan | Line |
|---|---|
| `AggregateExec mode=SinglePartitioned` (not split: its input is already partitioned on the group columns) | `MERGE_GROUP_BY cols none` |
| `AggregateExec mode=FinalPartitioned` (fed through a hash boundary) | `MERGE_GROUP_BY cols hash` |
| `HashJoinExec mode=Partitioned` | `HASH_JOIN l=r hash` |
| `HashJoinExec mode=CollectLeft` under a `NetworkBroadcastExec` | `HASH_JOIN l=r broadcast` (`l` on the build side) |
| any join in a stage of one task | `HASH_JOIN l=r hash width=1` |

A site whose key is not a plain column is left out, and so is an aggregate with grouping sets or in
a stage of one task. Columns are named as DataFusion's plan names them; a group column the query
renames (`n_name AS nation`) does not match the scan column Sirius reads, and Sirius decides that
site itself.

DFD is pinned to commit `9b62f20b` (crate 4.0.0, DataFusion 55).
