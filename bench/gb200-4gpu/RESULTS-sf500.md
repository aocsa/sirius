# MEASURED — Engine A @ SF500 on `presto-gb200-gcn-17`

**Date:** 2026-08-12 · **HEAD:** `4e6439c8` (demo-multi-cn) · **Executed:** [`PLAN.md`](PLAN.md) Engine A arm
**Data:** `/raid/prestouser/aocsa/tpch_parquet_sf500` (132 GB, ext4 on `/dev/md0`)
**Raw:** `../../../benchmark-results/sf500-engineA-failhunt/` and `…-retest/`

**First 22-query-class Engine A sweep ever run at SF500.** Prior SF500 coverage was 4 queries.

---

## Headline

**The plan works end to end.** Cluster launched, 4 CNs alive, blacklist settled empty,
`enable_pipeline_engine` set and read back, 17 queries executed, clean teardown to
204/83/39/316 MiB. Exit 0, no manual intervention.

**14 of 17 pass. 3 fail — and they are three *different* bugs, not one.**

| Run | Config | Result |
|---|---|---|
| Sweep | `GPU_MEM=159GiB STAGING=16GiB`, 17 q × (cold+warm) | 29 pass · 4 refused · 1 empty |
| Retest | `GPU_MEM=140GiB STAGING=32GiB`, 3 q × (cold+3 warm) | 5 pass · 2 refused · 3 empty |

---

## The 14 that pass

`q01 q02 q03 q04 q06 q07 q11 q12 q13 q14 q16 q19 q20 q22` — all in **0.5–4.9 s**, cold and warm.

| q | cold | warm | | q | cold | warm |
|---|---:|---:|---|---|---:|---:|
| q01 | 4894 | 3975 | | q13 | 1103 | 1094 |
| q02 | 1036 | 876 | | q14 | 1365 | 1274 |
| q03 | 1394 | 1346 | | q16 | 559 | 528 |
| q04 | 836 | 802 | | q19 | 1384 | 1354 |
| q06 | 976 | 961 | | q20 | 1854 | 1913 |
| q07 | 2460 | 3049 | | q22 | 530 | 555 |
| q11 | 866 | 790 | | q12 | 1021 | 995 |

**q11 returned 467,405 rows = 5.04× the SF100 count** — the `0.0001/SF` spec fraction scaled
correctly. **q16 returned 27,840 rows, identical to SF100** — expected, its cardinality is bounded
by the brand × type × size space and does not grow with scale.

> ⚠️ `pass` means **completed with a plausible row count**. The harness never compares values, and
> the oracle diff has not been run. The 7 decimal-defect queries are still wrong where they say pass.

---

## Failure 1 — q17: arena sizing. **FIXED.**

```
16 GiB arena: refused ×2  (2720, 2563 ms)
32 GiB arena: pass    ×4  (3261, 3156, 3116, 3096 ms)
```

```
exchange staging arena exhausted: requested 1215987328 bytes,
929767936 free of 17179869184 capacity with 13 leases outstanding
(raise SIRIUS_EXCHANGE_STAGING_BYTES)
```

Not GPU memory — the 159 GiB pool was never the constraint. **A knob, not a wall.**
**Action: `STAGING=32GiB` is the new baseline for SF500.**

---

## Failure 2 — q21: **NOT fixed by doubling the arena.** This is the real finding.

```
16 GiB arena: refused — 13–25 leases outstanding, ~1.0–1.2 GiB each
32 GiB arena: refused — 32–36 leases outstanding, ~768 MB each
```

**Doubling the arena doubled the outstanding lease count instead of letting the query finish.** The
arena fills completely, whatever its size, and then the next request fails. That is not a sizing
shortfall — a query that needs *N* bytes succeeds once you give it *N*. This behaves like
**leases not being released while the query runs**, which matches the known q09 signature
(*"staging-arena exhaustion with 75 leases leaked across runs"*).

Lower bound on demand: ~36 × 768 MB ≈ **27 GB of concurrent staging**, and still short.

> **This invalidates a recommendation I made earlier.** The A100 config
> ([`../a100x8/engine-a-sirius.yaml`](../a100x8/engine-a-sirius.yaml)) budgets an **8 GiB** arena on
> an 80 GiB card, reasoning that 16 GiB was 20% of the device. Measurement says q21 needs **>32 GiB
> on a 185 GiB card**. An 80 GiB A100 cannot give q21 a large enough arena at any setting —
> **q21 (and probably q17) will fail on the A100 box regardless of tuning**, until the lease
> behaviour is fixed. The A100 config has been annotated accordingly.

**Action: root-cause the lease lifecycle before raising the arena further.** Raising it is not
working, and on smaller cards it is not even available.

---

## Failure 3 — q15: silent, non-deterministic wrong answer. **The most serious.**

| Run | 16 GiB arena | 32 GiB arena |
|---|---|---|
| cold | **pass** (1 row) | **empty** (0 rows) |
| warm 1 | **empty** | **pass** (1 row) |
| warm 2 | — | **empty** |
| warm 3 | — | **empty** |

**1 pass in 4.** It flips irregularly, **independent of run order and arena size** — the cold run
passed in one config and failed in the other. Not degradation, not memory: **non-determinism.**

No error. `q15.r1.out` is **0 bytes**. Exit code 0. The harness scores it `empty`, not `wedge` —
the query completed in normal time (2283–3295 ms) and returned nothing.

### Mechanism

```sql
revenue AS (SELECT l_suppkey, sum(l_extendedprice * (1 - l_discount)) AS total_revenue …)
SELECT … WHERE total_revenue = (SELECT max(total_revenue) FROM revenue)
```

The predicate is an **exact equality on a floating-point value**, and that value comes from exactly
the expression `expr_translator.rs:459-481` (`translate_arithmetic`) lowers to **FP64**. `revenue`
is referenced twice. If the two evaluations reduce in different orders — which parallel GPU
aggregation does not guarantee — the sums differ in the last bits and the equality **matches
nothing**.

**Under exact decimal arithmetic this is impossible**: decimal addition is associative, so any
summation order yields an identical result and the equality always matches.

> **This escalates the decimal defect.** It was documented as a ~0.1% value error on 7 queries.
> On q15 it silently produces the **wrong row count**, intermittently, with no error and exit 0 —
> and a 1-run sweep will call it `pass` roughly a quarter to half the time. A benchmark that runs
> each query once would report q15 as passing and publish a fabricated timing.

**Action: q15 must not be quoted at all until `translate_arithmetic` is fixed.** It is not
"timing-only" — its *timings* are also meaningless, because a run returning 0 rows did less work.

---

## Predictions tested

| Prediction | Verdict |
|---|---|
| **q13** blows the 2³¹ chars-per-string-column cap (~4.2× over at SF500) | ❌ **Refuted** — passed both runs, ~1.1 s. The column evidently does not materialize whole per CN |
| **q17** staging-arena pressure | ✅ **Confirmed** — and fixable with a knob |
| **q21** staging-arena pressure | ✅ **Confirmed**, ❌ but **not fixable by sizing** |
| **q07** high risk (6 tables, 5 joins) | ❌ **Refuted** — passed, 2.5/3.0 s |

---

## Revised tiering, measured

| Tier | Queries | Change |
|---|---|---|
| Headline | `q02 q04 q06 q11 q12 q16 q20 q22` | unchanged — **all 8 pass at SF500** |
| Promote to headline candidate | **`q13`** | passes; was Tier 4 on a refuted projection |
| Timing-only (values wrong) | `q01 q03 q07 q14 q19` | q07 completes reliably; still FP64-wrong |
| **Excluded — unusable** | **`q15`** | non-deterministic row count |
| Blocked on arena | `q17` (works at 32 GiB), `q21` (blocked) | |

---

## Immediate actions

1. **`STAGING=32GiB` as the SF500 baseline** — recovers q17 for free.
2. **Root-cause the staging-lease lifecycle** (q21). Sizing does not fix it, and the A100 box
   cannot out-size it. This now gates the A100 plan, not just q21.
3. **Fix `translate_arithmetic`** — one function; it now provably causes a *silent wrong row count*,
   not just a small value error. Highest leverage item on the board.
4. **Run the DuckDB oracle diff** at relative tolerance 1e-12. Nothing above is a correctness claim.
5. **Never run a 1-iteration sweep** — q15 would report `pass`.
