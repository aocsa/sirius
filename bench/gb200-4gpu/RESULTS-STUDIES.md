# Results by study — `presto-gb200-gcn-17`, TPC-H SF500

**Executed:** 2026-08-12 · **Plan:** [`PLAN.md`](PLAN.md) · **Engine A detail:** [`RESULTS-sf500.md`](RESULTS-sf500.md)
**Raw:** `benchmark-results/sf500-studies-2-3/` · `…/sf500-engineA-failhunt/` · `…/sf500-engineA-retest/`

| # | Study | Engines | Regime planned | Regime **run** | Status |
|---|---|---|---|---|---|
| **1** | Scale-Out | Sirius only | pinned/hot | — | ❌ **NOT RUN** |
| **2** | GPU Shootout | Sirius, cudf-polars | cold | **warm** ⚠ | ✅ done |
| **3** | Cost Efficiency | Sirius, StarRocks | warm | warm ✅ | ✅ done |

> **Note on Study 1's question.** The brief asks *"2 → 4 → 8 GPUs."* **This box has 4 GPUs**, so
> scale-out here is **1 → 2 → 4**. The 8-GPU arm requires the A100 box
> ([`../a100x8/`](../a100x8/)). Only the 4-CN point exists today.

---

## Study 1 — Scale-Out ❌ NOT RUN

**Question:** Does Sirius scale 1 → 2 → 4 GPUs? *(not 2 → 4 → 8 — this box has 4)*
**Engines:** Sirius only · **Regime:** pinned/hot

| Arm | CNs | GPUs | Status |
|---|---|---|---|
| 1-GPU | 1 | GPU0 | not run |
| 2-GPU | 2 | GPU0 + GPU2 | not run |
| 2-GPU NUMA variant | 2 | GPU0 + GPU1 | not run |
| **4-GPU** | 4 | GPU0–3 | ✅ measured (3× independently) |

**What exists:** the 4-CN point only — 15/17 queries, 7,397 ms over the headline 8. A single
point is not a scaling curve, so **no scaling claim can be made yet.**

**To complete:** three more sweeps (`NUM_CNS=1`, `NUM_CNS=2`, and the GPU0+GPU1 variant). ~30 min.
Also unrun: the pinned regime — every measurement so far is unpinned.

---

## Study 2 — GPU Shootout ✅ Sirius vs cudf-polars

**Question:** Sirius vs cudf-polars, same box, no bias
**Regime planned:** cold · **Regime run:** ⚠ **warm — see the deviation below**

### Result

> ### cudf-polars is **2.29× faster** than Sirius, and wins **all 15 shared queries**

| Headline 8-query set | Total |
|---|---|
| **cudf-polars** | **3,224 ms** |
| Sirius | 7,397 ms |

**Coverage: cudf-polars 17/17, Sirius 15/17.** cudf-polars completed q15 and q21, which Sirius
cannot — so it wins on coverage *and* speed.

Per-query, cudf-polars ÷ Sirius (lower = cudf-polars faster). Not one Sirius win:

| q17 | q02 | q11 | q01 | q22 | q04 | q07 | q13 | q20 | q06 | q14 | q03 | q16 | q12 | q19 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0.18× | 0.19× | 0.22× | 0.34× | 0.34× | 0.35× | 0.39× | 0.44× | 0.45× | 0.46× | 0.46× | 0.60× | 0.65× | 0.77× | 0.98× |

### ⚠ Regime deviation — "cold" was not achieved

The plan specifies *cold, page cache dropped per run*. **Dropping the page cache requires root,
which is not available on this box.** What actually ran:

| Engine | Actual |
|---|---|
| Sirius | first-contact after cluster start |
| cudf-polars | `--io-mode hot --iterations 3` |

Both are **warm**, and symmetrically so — neither engine got a cache advantage. Using
cudf-polars' true `--io-mode cold` (which calls `kvikio.drop_file_page_cache`) while Sirius could
not drop caches would have been a *worse* asymmetry. **Report this as warm, not cold.**

### This reverses the earlier result

A previous chart showed Sirius beating cudf-polars. The audit traced that to quoting cudf-polars
from `--iterations 1`, carrying a 1.4–4.6× first-touch penalty. With a matched protocol the
ordering flips.

### The confound that must be measured before this is quoted

**Sirius's times include a MySQL round trip through the StarRocks FE** (parse → plan → distribute
→ collect). **cudf-polars' are `time.monotonic()` around an in-process `collect()`.** This is
divergence #4 from the original audit, which recorded it as *"asserted, not measured — it needs a
number."* It still has none.

It matters more here than at SF100: Sirius's fastest query is **501 ms** against cudf-polars'
**163 ms**. A fixed 200–300 ms coordinator cost would be a large share of the gap on short
queries — precisely where cudf-polars' lead is largest (q02 0.19×, q11 0.22×). It would not
overturn the result, but it could compress it materially.

**Cost to measure:** one `SELECT 1` through the FE, or Sirius-side fragment totals from telemetry
vs client wall clock. Minutes.

---

## Study 3 — Cost Efficiency ✅ Sirius vs StarRocks

**Question:** $ per run vs a CPU engine · **Regime:** warm ✅ as planned

### Result

> ### Sirius is **3.93× faster** than StarRocks — clearing the **1.50×** cost break-even by 2.6×

| Headline 8-query set | Total |
|---|---|
| Sirius | 7,397 ms |
| **StarRocks** | **29,037 ms** |

Sirius wins **all 15 queries it completes**, 1.31× (q11) to 11.41× (q16):

| q16 | q12 | q04 | q19 | q02 | q13 | q03 | q17 | q06 | q22 | q14 | q20 | q01 | q07 | q11 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 11.4× | 6.4× | 5.6× | 5.3× | 5.0× | 4.1× | 3.9× | 3.4× | 2.6× | 2.2× | 2.0× | 1.8× | 1.8× | 1.6× | 1.3× |

### Break-even — the primary result

This box is **on-prem with no $/hr**, so per [`PLAN.md`](PLAN.md) the honest framing is the
break-even ratio, not modeled dollars:

| Sirius must beat | by | Delivered | Verdict |
|---|---|---|---|
| `m8i.48xlarge` ($12.19/hr) | 1.09× | **3.93×** | ✅ clears by 3.6× |
| `m8gd.48xlarge` ($8.83/hr) | 1.50× | **3.93×** | ✅ clears by 2.6× |

14 of 15 queries individually clear the m8gd bar; only q11 (1.31×) falls below it.

### ⚠ The core-count caveat

**StarRocks ran on this box's 144 Grace cores, not on a 192-vCPU AWS instance.** `m8gd.48xlarge`
has **33% more cores**. Scaling optimistically-linearly, StarRocks there might reach ~21.8 s,
putting Sirius at **~2.95×** — still comfortably past the 1.50× bar, but the headline 3.93× is
*this box's CPU*, not m8gd's.

The upside of running B here: the **timing** comparison is on identical hardware, same data, same
filesystem — cleaner than any cloud pairing. Only the **cost** axis is modeled, and it must be
labelled so.

### StarRocks completed 17/17

Including q15 and q21, which Sirius fails. Every aggregate above is restricted to the shared
8-query headline set — comparing a 15-query Sirius total against a 17-query StarRocks total would
flatter Sirius by dropping its two hardest cases.

---

## Cross-study summary

| | Sirius | cudf-polars | StarRocks |
|---|---|---|---|
| Headline-8 total | 7,397 ms | **3,224 ms** | 29,037 ms |
| Coverage | **15/17** | 17/17 | 17/17 |
| vs Sirius | — | **2.29× faster** | 3.93× slower |

**Ordering: cudf-polars > Sirius > StarRocks.**

Sirius's cost case against a CPU engine is strong and holds up. Its case against another GPU
engine does not, on this box, at this scale factor, with this protocol.

## What would change these numbers

| Action | Expected effect |
|---|---|
| **Measure FE + client overhead** | Could compress Study 2's 2.29×. Unmeasured — do this first |
| **Fix `translate_arithmetic`** | Headline set 8 → 14 queries; removes the ⚠ from 6 timings |
| **Root-cause the staging-lease lifecycle** | Recovers q21 → coverage 16/17 |
| **Run Study 1** | The missing study |
| **Run the DuckDB oracle diff** | Nothing above is currently a correctness claim |
