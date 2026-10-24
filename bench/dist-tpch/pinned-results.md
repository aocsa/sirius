# Distributed TPC-H with pinned tables: SF3000 and SF10000

Measured 2026-10-24 on one host with 4× NVIDIA GB200 (189 GB each, 144 Grace cores, 956 GiB CPU memory in two NUMA nodes), one Sirius rank per GPU. Build: branch `dist-plan-opt-rank-pin` at f0e88990. The data is Parquet on GPFS, read with O_DIRECT.

Each suite pins its tables once at startup, on every rank, then runs all 22 queries 3 times in the same processes. The first run is cold, and the best of the other two is warm. Every answer is checked against an unpinned run of the same query.

## How the tables are pinned

- **Rank share:** by default each rank pins its share of a table's files, the byte ranges its scans read. The ranks together hold the table once.
- **Replicated:** `replicate = true` pins the whole table on every rank. Joins build from it without exchanging it. Use it for small tables.
- **Compression:** with a plan directory set, each table that has a Simpatico plan there is pinned compressed. Others are pinned uncompressed.
- **Narrowing:** on by default. It stores each column in the smallest type its values fit. The ranks agree on one type per column when they pin.
- **Memory limit:** a pin fails, on every rank, rather than fill more than `pin_table_memory_limit` (0.9) of the memory it pins into, or of its CPU NUMA node. The queries then read that table from disk.
- **Pin order:** `run_suite.py` pins the smallest tables first, so a table that doesn't fit is the largest.

## Setup

Follow the [README](README.md) to build and to make the unpinned reference runs (`CASES=direct`). The compression plans come from the repo:

```bash
mkdir -p plans-sf10k plans-small
P=src/compression/simpatico_codegen/plans/tpch_sf1000
cp $P/lineitem.txt $P/orders.txt plans-sf10k/
cp $P/partsupp_disabled.txt plans-sf10k/partsupp.txt
cp $P/supplier_disabled.txt plans-sf10k/supplier.txt
cp plans-sf10k/partsupp.txt plans-sf10k/supplier.txt plans-small/
```

The partsupp and supplier plans are disabled in the repo; copying them under the table's name enables them. The customer and part plans stay disabled.

Configs, in this folder:
- `host-pin.yaml`: cache `none`, 32 scan threads, 60 GB spill. The host pool comes from `--host-pool`.
- `direct-io.yaml`: the same, with a 250Gi host pool and 30 GB spill.

Host memory: two ranks share each 478 GiB CPU node, so each rank's host pool must stay below about 210Gi. The GPUs' memory also shows up as NUMA nodes and in `free`; don't count it.

## SF3000: every table pinned on the host

| Table | Columns | How |
|---|---|---|
| lineitem | l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode | rank share, compressed |
| orders | o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_shippriority (no o_comment) | rank share, compressed |
| customer | c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment | rank share |
| part | p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container | rank share |
| partsupp | ps_partkey, ps_suppkey, ps_availqty, ps_supplycost | rank share |
| supplier | s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment | replicated |
| nation | n_nationkey, n_name, n_regionkey | replicated |
| region | r_regionkey, r_name | replicated |

These are the columns the 22 queries read, except o_comment. Leaving it out keeps orders small, so Q13 reads orders from disk.

```bash
pixi run python bench/dist-tpch/run_suite.py --out OUT --tag _pinall \
  --config bench/dist-tpch/host-pin.yaml --host-pool 210Gi --data /path/to/tpch_sf3000 \
  --sf 3000 --pin-tier host --pin-plans src/compression/simpatico_codegen/plans/tpch_sf1000 \
  --pin-exclude o_comment --pin-replicate nation,region,supplier --ref-dir DIRECT_OUT
```

- **Pin time per rank:** 57 s for all 8 tables. lineitem took 43 s and orders 4.8 s; each of the others took less than 4 s.
- **Answers:** all 22 match the unpinned answers.
- **Time:** 23.2 s warm for all 22 queries, against 207.6 s unpinned. That's 8.9× faster.

Times in seconds:

| Query | Unpinned cold | Unpinned warm | Pinned cold | Pinned warm | Warm speedup |
|---|---|---|---|---|---|
| Q1 | 18.16 | 5.06 | 1.27 | 1.17 | 4.3x |
| Q2 | 22.73 | 4.46 | 0.43 | 0.39 | 11.6x |
| Q3 | 52.77 | 10.80 | 0.79 | 0.72 | 14.9x |
| Q4 | 30.34 | 8.54 | 0.55 | 0.54 | 15.7x |
| Q5 | 38.37 | 11.40 | 0.77 | 0.76 | 15.0x |
| Q6 | 28.81 | 7.75 | 0.44 | 0.43 | 17.8x |
| Q7 | 65.02 | 14.47 | 0.86 | 0.83 | 17.4x |
| Q8 | 39.40 | 9.99 | 0.93 | 0.94 | 10.6x |
| Q9 | 56.39 | 13.30 | 1.99 | 1.86 | 7.2x |
| Q10 | 39.92 | 11.64 | 1.33 | 1.32 | 8.8x |
| Q11 | 27.86 | 3.54 | 1.36 | 1.36 | 2.6x |
| Q12 | 35.89 | 9.77 | 0.95 | 0.96 | 10.2x |
| Q13 | 10.07 | 3.99 | 3.63 | 3.33 | 1.2x |
| Q14 | 57.50 | 8.77 | 0.70 | 0.65 | 13.5x |
| Q15 | 42.47 | 8.24 | 0.64 | 0.63 | 13.1x |
| Q16 | 25.48 | 1.64 | 0.48 | 0.46 | 3.6x |
| Q17 | 49.16 | 12.24 | 0.75 | 0.76 | 16.1x |
| Q18 | 47.19 | 13.93 | 2.15 | 2.16 | 6.5x |
| Q19 | 71.90 | 11.69 | 0.99 | 0.95 | 12.2x |
| Q20 | 34.41 | 6.84 | 0.93 | 0.93 | 7.4x |
| Q21 | 87.43 | 27.05 | 1.82 | 1.78 | 15.2x |
| Q22 | 9.52 | 2.46 | 0.28 | 0.27 | 9.2x |
| **Total** | **890.8** | **207.6** | **24.0** | **23.2** | **8.9x** |

The unpinned runs are `run_tpch.sh` `direct` runs, one process per query.

## SF10000: what fits

Pinning every table at SF10000 would need about 757 GB per rank even compressed. That's far more than a host pool and a GPU hold. Four pin sets were compared:

| Set | Tier, pool, config | Tables | Pin time per rank |
|---|---|---|---|
| A: build before 445a6640, narrowing off | GPU, 250Gi, `direct-io.yaml` | partsupp and supplier compressed, rank share | 14.8 s |
| B: rank share | GPU, 250Gi, `direct-io.yaml` | partsupp and supplier compressed; nation, region; all by rank share | 8.8 s |
| C: replicated | GPU, 250Gi, `direct-io.yaml` | as B, with supplier, nation and region replicated | 8.8 s |
| D: + orders | host, 190Gi, `host-pin.yaml` | as C, plus orders compressed (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_shippriority) | 24 s, plus a lineitem pin that didn't fit |

All sets pin every column of partsupp and supplier the queries read: ps_partkey, ps_suppkey, ps_availqty, ps_supplycost; and s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment.

```bash
# B (C: add --pin-replicate supplier,nation,region)
pixi run python bench/dist-tpch/run_suite.py --out OUT --tag _shared \
  --config bench/dist-tpch/direct-io.yaml --data /path/to/tpch_sf10000 --sf 10000 \
  --pin-tier gpu --pin-plans plans-small --pin-only partsupp,supplier,nation,region --ref-dir DIRECT_OUT

# D
pixi run python bench/dist-tpch/run_suite.py --out OUT --tag _hot \
  --config bench/dist-tpch/host-pin.yaml --host-pool 190Gi --data /path/to/tpch_sf10000 --sf 10000 \
  --pin-tier host --pin-plans plans-sf10k --pin-only lineitem,orders,partsupp,supplier,nation,region \
  --pin-replicate supplier,nation,region \
  --pin-exclude l_tax,l_returnflag,l_linestatus,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,o_comment \
  --ref-dir DIRECT_OUT
```

In D, lineitem's 7 most-read columns didn't fit in what the other tables left of the pool. They were l_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice, l_discount and l_shipdate. After 32 s the pin would have filled 174 of the 190 GiB, so the memory limit failed it on every rank, and the queries read lineitem from disk.

Warm times in seconds. All answers match the unpinned ones. The last column counts the scans each query in D served from pins, summed over the ranks:

| Query | Unpinned, old build | A | B | C | D | Scans from pins in D |
|---|---|---|---|---|---|---|
| Q1 | 18.9 | 18.40 | 18.92 | 17.18 | 19.44 | 0 |
| Q2 | 13.6 | 1.33 | 1.28 | 1.31 | 1.49 | 84 |
| Q3 | 34.5 | 20.78 | 20.24 | 20.39 | 16.30 | 24 |
| Q4 | 27.2 | 14.39 | 14.39 | 15.09 | 11.97 | 12 |
| Q5 | 35.9 | 23.59 | 23.30 | 23.51 | 20.07 | 48 |
| Q6 | 27.2 | 13.55 | 13.55 | 14.45 | 13.27 | 0 |
| Q7 | 44.9 | 26.31 | 25.91 | 25.29 | 21.94 | 48 |
| Q8 | 33.7 | 32.22 | 31.45 | 31.19 | 28.07 | 60 |
| Q9 | 45.6 | 33.83 | 33.65 | 33.67 | 30.90 | 48 |
| Q10 | 38.2 | 24.09 | 24.22 | 23.97 | 20.18 | 24 |
| Q11 | 11.5 | 4.12 | 4.05 | 3.97 | 4.23 | 68 |
| Q12 | 33.0 | 16.83 | 16.00 | 16.50 | 14.04 | 16 |
| Q13 | 15.0 | 14.32 | 13.83 | 13.49 | 14.08 | 0 |
| Q14 | 29.8 | 18.28 | 18.40 | 19.06 | 18.40 | 0 |
| Q15 | 29.7 | 17.53 | 17.60 | 17.86 | 17.59 | 12 |
| Q16 | 6.6 | 1.50 | 1.50 | 1.59 | 1.60 | 24 |
| Q17 | 36.8 | 27.81 | 27.76 | 29.79 | 28.39 | 0 |
| Q18 | 48.1 | 31.89 | 32.37 | 32.51 | 26.59 | 12 |
| Q19 | 39.2 | 20.11 | 20.99 | 20.00 | 20.95 | 0 |
| Q20 | 22.0 | 19.63 | 19.25 | 19.37 | 19.14 | 36 |
| Q21 | 81.5 | 47.22 | 46.16 | 45.65 | 44.04 | 36 |
| Q22 | 7.2 | 4.06 | 4.17 | 4.14 | 2.42 | 12 |
| **Total** | **680.1** | **431.8** | **429.0** | **430.0** | **395.1** | |

Cold totals: A 481.4 s, B 476.2 s, C 478.7 s, D 419.8 s.

- **Old-build unpinned column:** the old build's sweep of the full dataset, with one process per query (see the [README](README.md)). A–D run in one process, so part of their gain over it comes from that.
- **B against A:** the same speed. The difference is that nation and region are now pinned with narrowing on, which failed before. Pins are also faster, because a pin now reads files and decodes batches several at a time.
- **C against B:** replicating the small tables saves nothing measurable here. Broadcasting them was already cheap; the time goes into scanning lineitem and orders.
- **D:** pinning orders makes the 22 queries 8.5% faster than A (395 s against 432 s). The queries that join orders gain most: Q3, Q4, Q5, Q7, Q10, Q18 and Q22. To gain more, lineitem has to fit. That needs either GPU memory for the pins as well (the runner pins every table on one tier), or fewer lineitem columns.

## Pin speed and the memory limit

Same host, data on GPFS, times per rank:

| Pin | Before | Now |
|---|---|---|
| SF3000 lineitem, 4 columns (Q6), host | 39.5 s | 17.8 s |
| SF3000 lineitem, 14 columns compressed, host | 36–37 s, one batch at a time | 26.5–31 s |
| SF10000 partsupp, 4 columns compressed, GPU | 14.3 s | 7.3–8.4 s |

The uncompressed SF10000 Q6 lineitem pin doesn't fit within the memory limit with two ranks per CPU node:
- **250Gi pools:** NUMA node 0 would have been left 46 of its 478.5 GiB free.
- **200Gi pools:** the pin would have filled 180.5 of the 200 GiB.

Either way the pin failed cleanly on every rank and Q6 read lineitem from disk. The old build ran the host out of memory with this pin at 250Gi.

## Answers that differ only in ties

Q3 at both scale factors, and Q18 at SF10000, return rows that differ from the reference in tied rows only. The generated data repeats values: in Q3 several orders share the same revenue and date, and in Q18 one customer has several orders with the same date, price and quantity. The tied orders come back in another order, and where more of them tie than the LIMIT keeps, a different subset is returned. Every other column matches.
