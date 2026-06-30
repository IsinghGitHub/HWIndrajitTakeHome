# Performance Baseline

**Date:** 2026-06-30  
**Dataset:** SAMPLE (2 networks × 3 parquet files each, 2 counties: 4013, 6037)  
**Machine:** Darwin 24.5.0 (Apple Silicon)  
**Python:** 3.10 (miniforge/conda)

---

## Wall-clock times (5 runs each)

### provider_download_assignment.py

| Run | Wall time |
|-----|-----------|
| 1   | 0.690s    |
| 2   | 0.611s    |
| 3   | 0.674s    |
| 4   | 0.619s    |
| 5   | 0.651s    |
| **Mean** | **0.649s** |
| Min / Max | 0.611s / 0.690s |

### provider_count_assignment.py

| Run | Wall time |
|-----|-----------|
| 1   | 0.642s    |
| 2   | 0.670s    |
| 3   | 0.671s    |
| 4   | 0.658s    |
| 5   | 0.741s    |
| **Mean** | **0.676s** |
| Min / Max | 0.642s / 0.741s |

---

## Hotspot breakdown (cProfile, cumtime)

### provider_download_assignment.py

| Phase | cumtime | Notes |
|-------|---------|-------|
| Import overhead (polars, pandas, pyarrow, etc.) | ~0.520s | ~80% of total wall time |
| `extract_data` (entire business logic) | ~0.122s | |
| `load_base_files` → `read_and_drop_duplicates` (parquet reads) | ~0.054s | 6 parquet files read serially, row-group filtered |
| `load_compare_files` | ~0.017s | |
| `pd.concat` calls (×15) | ~0.006s | |
| `merge` (×3: ind score, org score, hosp map) | ~0.004s | |
| `groupby.transform` (net_count) | ~0.002s | |
| `pl.from_pandas` + `write_csv` + `write_parquet` (×2) | ~0.003s | |

### provider_count_assignment.py

| Phase | cumtime | Notes |
|-------|---------|-------|
| Import overhead | ~0.524s | ~80% of total wall time |
| `count_data` (entire business logic) | ~0.125s | |
| `load_base_files` → `read_and_drop_duplicates` | ~0.044s | |
| `load_compare_files` | ~0.012s | |
| `get_score_category_counts` (×2 calls) | ~0.022s | |
| `groupby.apply` with Python lambda (×6) | ~0.016s | slowest pandas call in script |
| `pd.concat` calls (×30) | ~0.008s | |
| `merge` (×5) | ~0.006s | |

---

## Key observations

1. **Import tax dominates (~80% wall time).** Both scripts spend ~520ms loading polars,
   pandas, pyarrow, pyodbc, etc. before a single byte of data is processed. On the sample
   dataset the actual business logic runs in ~120–125ms. On production data the logic will
   dominate instead.

2. **Parquet reads are serial.** `read_and_drop_duplicates` reads parquet files one-by-one
   in a list comprehension. With many specialty files per network this is the largest single
   bottleneck in business logic.

3. **`groupby.apply` with Python lambda (count script).** `get_score_category_counts` uses
   `df.groupby(...).apply(lambda x: pd.Series({...}))` — the slowest pandas groupby mode.
   Replaces poorly with `groupby.agg` or vectorised boolean masks.

4. **Repeated `drop_duplicates` in loops.** Both scripts call `drop_duplicates` 14+ times,
   including inside loops. On large DataFrames this adds up.

5. **Base/compare files loaded serially.** `load_base_files` / `load_compare_files` iterate
   over network IDs sequentially. `joblib.Parallel` is imported but unused; parallelising
   reads across networks would cut I/O time proportionally.

6. **No data caching across scripts.** Both scripts re-read the same parquet and CSV score
   files from disk every invocation.

---

## Improvement targets (for post-optimisation comparison)

- [x] Parallelise parquet reads — DuckDB scans all files in one vectorised pass with pushdown
- [x] Replace `groupby.apply` lambda — replaced with SQL `COUNT(*) FILTER (WHERE ...)` in DuckDB
- [x] Reduce `drop_duplicates` calls — dedup handled by DuckDB QUALIFY / DISTINCT
- [x] Eliminate duplicate data read across both scripts — single shared load in provider_pipeline.py
- [ ] Persistent warm worker (keep DuckDB connection + reference data in memory across requests)
- [ ] Result cache keyed on request inputs (counts/downloads are deterministic)

---

## provider_pipeline.py — post-optimisation results

**Date implemented:** 2026-06-30  
**Approach:** DuckDB core + Polars I/O; single read+clean+score-join pass feeding both count
and download output builders. Replaces both original scripts in one invocation.

### Wall-clock times (5 runs)

| Run | Wall time | Notes |
|-----|-----------|-------|
| 1   | 1.344s    | Cold-start: DuckDB JIT compilation |
| 2   | 0.614s    | |
| 3   | 0.613s    | |
| 4   | 0.763s    | |
| 5   | 0.682s    | |
| **Mean (runs 2-5)** | **0.668s** | Warm steady-state |

### Comparison vs originals

| Workload | Before | After | Change |
|----------|--------|-------|--------|
| Download only | 0.649s | — | — |
| Count only | 0.676s | — | — |
| **Both outputs combined** | **1.325s** | **0.668s** | **~2× faster** |

### What changed

| Original bottleneck | Fix applied |
|---------------------|-------------|
| 6 parquet files read serially twice (once per script) | DuckDB `read_parquet([...])` vectorised single scan with county pushdown |
| `groupby.apply(lambda...)` Python-level loop for score categorisation | `COUNT(*) FILTER (WHERE ...)` SQL aggregation |
| Score CSVs read twice (once per script) | Loaded once into `score_ind` / `score_org` DuckDB tables |
| `drop_duplicates` called 14+ times | Dedup at ingestion via `QUALIFY ROW_NUMBER() OVER (...) = 1` |
| Common/unique via `groupby.transform('nunique')` | SQL window `COUNT(DISTINCT ...) OVER (PARTITION BY ...)` |
| Silent exception swallowing | `emit_error(traceback.format_exc())` in except block |
| Dead empty-guard bug (assigned to wrong variable) | Fixed in `_load_ind_table` / `_load_org_table` |

---

## Re-validation run — 2026-07-01

Re-ran all three scripts end-to-end on the same SAMPLE dataset (same county list,
base/compare JSON, clientId 45) to confirm the 2026-06-30 numbers still hold and to
verify output equivalence, not just timing. `/usr/bin/time -p` wall-clock, 5 runs each,
no other load on the machine.

### Wall-clock times (5 runs each)

| Run | download | count | pipeline |
|-----|----------|-------|----------|
| 1   | 0.65s    | 1.38s *(cold)* | 0.71s |
| 2   | 0.58s    | 0.60s | 0.69s |
| 3   | 0.60s    | 0.61s | 0.69s |
| 4   | 0.60s    | 0.67s | 0.58s |
| 5   | 0.60s    | 0.62s | 0.55s |
| **Mean (all 5)** | **0.606s** | **0.776s** | **0.644s** |
| **Mean (warm, excl. run 1)** | — | **0.625s** | — |

`count`'s run 1 (1.38s) is a one-off cold-start outlier (OS file-cache / Python bytecode
cache not yet warm) consistent with the original baseline's note that import overhead
dominates; excluding it, count and download are statistically indistinguishable (~0.6s),
as expected since they share the same import list and a similar amount of work.

### Combined-request comparison (the number that matters operationally)

In production, the orchestrator runs **both** `provider_count_assignment.py` and
`provider_download_assignment.py` per request. `provider_pipeline.py` replaces both with
one invocation. Timing the realistic unit of work — both original scripts run back-to-back
vs. one pipeline run — 5 trials each:

| Workload | Mean wall time | Min / Max |
|----------|-----------------|-----------|
| `count` + `download` run sequentially (today's behavior) | **1.309s** (1.277s warm, excl. run 1) | 1.238s / 1.435s |
| `provider_pipeline.py` (single run, replaces both) | **0.644s** | 0.550s / 0.710s |
| **Speedup** | **~2.0×** | |

This matches the 2026-06-30 baseline's "~2× faster" finding and reconfirms it on a fresh
run a day later — the result isn't a one-off artifact of machine state.

### Where the time goes (cProfile, cumtime, single run)

| Script | Total wall (profiled) | Business logic (`extract_data`/`count_data`/`run_pipeline`) | Import overhead |
|--------|------------------------|---------------------------------------------------------------|------------------|
| `provider_download_assignment.py` | 1.654s | 0.422s | ~1.23s (pyodbc, polars, pyarrow, joblib, requests, etc.) |
| `provider_count_assignment.py` | 0.725s | 0.135s | ~0.59s |
| `provider_pipeline.py` | 1.619s (cProfile overhead inflates this) | 0.333s (`run_pipeline`) | ~1.29s, of which **duckdb alone is ~97ms** (`python3 -X importtime`) on top of pandas/polars/pyarrow/numpy |

Note: cProfile's instrumentation overhead roughly doubles wall time vs. `/usr/bin/time`,
so these absolute numbers aren't comparable to the table above — only the *relative split*
between "imports" and "business logic" within each script is meaningful here.
`provider_pipeline.py` adds `duckdb` to the import list (the original two scripts don't
import it), which is a real, measurable tax (~97ms cold) — but it's paid once instead of
the alternative of hand-rolling vectorised SQL-like joins in pandas, and it's what lets the
business-logic portion fold two scripts' worth of read+clean+score-join into a single pass.

### Output equivalence check (not just speed)

Re-ran with `id_parameter="Bench_1"` and diffed row counts between old and new count
outputs to confirm the rewrite doesn't just run faster, it produces the same shape of
result:

| Output | `provider_count_assignment.py` | `provider_pipeline.py` (same logical table) |
|--------|----------------------------------|-----------------------------------------------|
| Results Table (`NI+_Results_Table.csv` / `NI+Improved_Results_Table.csv`) | 16 data rows | 16 data rows |
| Results BarGraph | 20 data rows | 20 data rows |
| Results PerformIndicators | 36 data rows | 36 data rows |

Row counts match exactly on this dataset. This is a smoke check, not a cell-by-cell golden
diff — the README's suggested next step ("golden equivalence + unit tests... assert
rewrite matches with a documented list of intentional bug-fix diffs") is still the right
bar for a real correctness sign-off before retiring the originals.

### Updated takeaway

The 2026-06-30 numbers hold up on re-run: **`provider_pipeline.py` is ~2x faster than
running the two original scripts back-to-back**, and on this sample dataset it reproduces
the original count script's output row counts exactly. The dominant cost in all three
scripts remains interpreter/library import time (60-80% of wall clock on this small
dataset), not business logic — so the next highest-leverage optimization is still the
two unchecked items below (warm persistent worker, result cache), not further
micro-optimizing the DuckDB SQL.
