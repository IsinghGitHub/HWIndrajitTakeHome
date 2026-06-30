# Solution Approach 
Author: Indrajit Singh
Date: 30-Jun-2026

#### Summary: Fixes vs originals:

  1. Dead empty-guard: load_compare_files assigned to base_ind_df on empty — fixed.
  2. Empty/malformed compareParentOrgId — guarded with an early return. Correction made
     2026-07-01 after a code-level audit: `"".split(",")` never actually returns `[]` in
     Python, so the literal "NameError after the loop" scenario described below doesn't
     reproduce as written; the real failure on a blank compare id is a KeyError on an empty
     network lookup key, caught by the surrounding try/except instead. Also, this only
     affected `extract_data` (download script) — `count_data`'s except block already logs
     a traceback in the unmodified original, so it wasn't actually broken the same way.
  3. Swallowed exception in `extract_data` (download script) — now calls emit_error + traceback.
  4. groupby.apply Python lambda in score categorization — replaced with SQL FILTER agg.
  5. Serial parquet reads — DuckDB reads all files in one vectorised scan with pushdown.
  6. Duplicate file reads across two scripts — eliminated; data loaded once.
  7. Organization/hospital download file was missing a `drop_duplicates()` after subsetting
     to its final column set, which let ~50% duplicate rows through — added back. Found via
     a post-implementation value-level audit (2026-07-01), not caught by the row-count-only
     equivalence check below.
  8. Market report (`NI+Improved_Results_PerformIndicators.csv`): the original only renames
     `MA Utilization Score` → `Utilization Score` on the individual market frame, not the
     org one, so Hospital Utilization Score in the Common Counties row silently sums to 0.
     This rewrite renames both — an intentional, disclosed output difference, not a bug.

See the comparison here : [performance_comparison data](./docs/PERFORMANCE_BASELINE.md). See
[Known output differences from the originals](#known-output-differences-from-the-originals)
below for items 7–8 and how they were found.

## Problem Deffinition

#### What the code actually does today (per request):

- Orchestrator spawns provider_count_assignment.py and provider_download_assignment.py as subprocesses, passing 13 sys.argv params (incl. JSON blobs for base/compare).

- Both scripts independently re-read the same per-network parquet (Individual) + CSV (Organization) files and the same score/market CSVs from disk, filter by county, dedupe, merge scores, aggregate.
- Counts script writes 3 tables to SQL Server via executemany; download script writes CSV+parquet to disk.


#### A few headline problems I can already see (I'll drill into each later): 

- Subprocess cold-start + re-importing pandas/polars/pyarrow every request
- The same reference data re-read on every request and duplicated across the two scripts 
- Joblib imported but never used (reads are sequential); row-wise .apply/groupby().apply() in hot paths; hardcoded SMTP creds;
- Bare except: swallowing failures; and a copy-paste bug where the empty-compare branch assigns to base_ind_df.
- (The SQL UPDATE-injection / DB-credentials risk is discussed separately below — it describes the brief's narrated production write path, which isn't code present in this repo.)


#### My recommended Solution: 

Re-architect the execution model. The subprocess-per-request design is the actual scalability ceiling — we pay interpreter cold-start + library import + full reference-data re-read on every single request, and we pay it twice because two scripts read the same data. Optimizing the pandas inside that model treats the symptom. 

I'd move to a persistent worker/service that keeps reference data warm in memory, run one read+filter+aggregate pass that feeds both the counts and the download outputs, and add a result cache keyed on the request inputs (counts/downloads are deterministic functions of their args). I'd reach for a lazy columnar engine (Polars lazy or DuckDB over the parquet lake) so filtering is pushed down instead of reading whole files into pandas.

#### I can suggest better engine runs the heavy read + join + aggregate. I would like to defend it as below.

❯ 1. DuckDB core + Polars I/O
     Relational logic as SQL over the parquet/CSV lake; automatic pushdown, vectorized out-of-core execution, precompute persists as parquet/DuckDB tables. Most maintainable + scales past memory.

Reason for Choosing DuckDB and Polars I/O:

    DuckDB is a high-performance, open-source analytical SQL engine. Its primary advantage is that it runs in-process (like SQLite) but is specifically optimized for analytics (like a data warehouse)

    Polars I/O operations are highly optimized, leveraging multi-threading, streaming, and the Apache Arrow columnar memory layout to read and write large datasets up to 10-20x faster than traditional libraries. 

  2. Polars lazy end-to-end
     LazyFrame scans with pushdown, collect once at the end. No new dependency, team already uses Polars, smaller leap from current code. Defensible on familiarity.

  3. Pandas, vectorized + warm
     Stay in pandas but kill row-wise .apply, dedupe reads, keep data warm. Lowest learning curve, but doesn't really fix the in-memory/scale ceiling I chose to re-architect away from.

  4. Spark / distributed
     Distributed engine for true horizontal data-parallelism. Overkill for this data size; heavy infra and operational cost; cold-start latency hurts the async-job UX.



Another important question is where's the line between data-load-time precompute and request-time compute that I would need to decide ?

> I would choose a clean per-network layer, pairwise at request, result cache on top. Which seems to be most visible redundancy in the current system.


Another important question is:

#### How do we handle the two outputs (counts vs downloads) that today are two scripts? 

So, right now  `provider_count_assignment.py` and `provider_download_assignment.py` re-read and re-clean the exact same provider/score data for the same request — the file walk, the parquet reads, the dedupe, the score joins are all duplicated across two processes. That's the most blatant waste in the system. They only truly diverge at the end: counts does distinct-NPI aggregations → 3 SQL tables; download emits row-level detail (more columns, plus the clientId in [45,5,8] column-set branching) → CSV+parquet.

- My recommended answer: 

    Option ( A) 
    
    one job, one scan of the clean layer, branch into two output builders — if counts and downloads are always produced together. The shared read/clean/score-join happens once; then a counts builder (aggregate) and a detail builder (project + client-specific columns) each consume it. That deletes an entire duplicate read+clean pass per request.

    The thing that would flip me to option (B) which is two jobs over a shared materialized layer: if download is on-demand — i.e., the user sees counts first and only sometimes clicks "download." Then they're separate request lifecycles and fusing them wastefully generates files nobody asked for. But even then, both jobs read the materialized clean layer, so neither touches raw files twice. 
    
    So the real question under the question is: are counts and downloads always generated together, or is download a separate on-demand action?

    I would choose option (A) ,which is  One job, two output builders
    Single scan of the clean layer feeds both a counts aggregator (→ SQL) and a detail builder (→ CSV/parquet). Deletes the duplicate read/clean pass. Best if counts + download always produced together.

    If we would have choosen two jobs, shared materialized layer
    
    It would keep counts and download as separate jobs, but both read per-network clean layer instead of raw files. Right if download is on-demand (user clicks after seeing counts) so you don't generate files nobody wants.

#### Note: 
Currently it is keeping it fully separate:

     Two independent scripts each doing their own full read/clean/join. Simplest diff from current code, but preserves the single most wasteful redundancy in the system.



#### Next question is : How do the results get persisted, now that one job produces both the counts (→ SQL) and the detail files? The current write path has several problems stacked together:

**Caveat added 2026-07-01**: the SQL write path described below (the f-string UPDATE,
`pyodbc.connect`, "pool connection" comment, etc.) is narrated from the assignment brief's
description of production behavior ("writes results to SQL Server... Driver={SQL Server}"),
not from code present in this repo — `provider_count_assignment.py`'s SQL write path is
already stubbed out (`# DB stubbed out — results are written to ./sample_data/output/
instead`) in every commit, including the first. So this is a forward-looking design risk for
whoever builds that write path against the real production code, not a finding I directly
observed by reading the given scripts.

Some of teh red flags identified (as narrated in the brief, not observed in this repo's code):

- SQL injection: cursor.execute("UPDATE [...] where RequestID={argument}".format(argument=id_parameter)) — id_parameter comes straight from sys.argv. The 3 INSERTs are parameterized, but this UPDATE is string-formatted.
- No idempotency: it INSERTs, then flips status, with no guard. You just chose a queue + worker pool (Q3) — which means retries. A retried job double-inserts every row, because nothing deletes/dedupes by RequestID first.
- No surrounding transaction: conn.commit() happens mid-stream; a crash between the inserts and the status update leaves a half-written, "in-progress-forever" request.
- Connection hygiene: a fresh pyodbc.connect with inline plaintext credentials per job, Driver={SQL Server} (the legacy driver), and a comment claiming "pool connection" with no pool.


My recommended answer: 

(A) Parameterized writes, wrapped in one transaction, made idempotent, over a pooled connection, with secrets pulled from env/secret-manager. Concretely: parameterize every statement (kill the f-string); make the write idempotent with a DELETE FROM ... WHERE RequestID = ? (or MERGE) before the inserts so a retried job is safe; wrap delete+insert+status-flip in a single transaction so the request row's status is always consistent with the data; reuse a pooled connection from the worker rather than reconnecting per job; and move the connection string + SMTP creds out of source into config/secrets. Volume is modest (counts are aggregated; detail goes to files, not SQL), so fast_executemany is fine — no need for BCP/staging-table machinery (C) unless the counts tables turn out to be huge.


 Parameterized, transactional, idempotent, pooled, secrets out. Now the dimension the brief names first — correctness — because the current code fails silently, and a re-architecture either consciously fixes that or inherits it.



I also found some concrete latent bugs while reading, not hypotheticals:

1. Dead empty-guard (both scripts). In load_compare_files, the if compare_ind_df.empty: branch assigns to base_ind_df, not compare_ind_df (count script line ~229, download line ~232). Copy-paste from the base loader. The intended empty-frame guard silently never applies to the compare side.
2. ~~NameError on empty compare list~~ — **corrected 2026-07-01**: I'd originally written this as "In extract_data, final_data_ind/final_data_org are only bound inside the for compareId loop, then used after it. If compareorgid_list is empty, those names don't exist → crash. The counts script has the same shape." A code-level audit found two things wrong with that: (a) `compareorgid_list` can't actually be `[]` via the documented CLI — `"".split(",")` returns `['']`, not `[]` — so this exact NameError doesn't reproduce; the real crash on a blank compare id is a `KeyError` on an empty network-lookup key, in `extract_data` only. (b) `count_data` does **not** have the same shape — `final_data_ind`/`final_data_org` there are reassigned after the loop from frames populated before it, so there's no analogous crash, and its except block already prints a traceback in the unmodified original. The practical takeaway (download script can fail with zero diagnostics on bad/empty compare input) still held and is fixed in `provider_pipeline.py`, just via a different mechanism (an outer try/except that emits a structured error) than originally described.
3. Swallowed failure. extract_data's except builds error_message then... does nothing — no print, no emit_error, no return. So it returns None → main prints "Failed" with zero diagnostics, and any output files already written are left orphaned.
4. Blanket blindness. warnings.filterwarnings("ignore") + bare except: (in send_email) hide real SettingWithCopyWarnings firing on genuine chained-assignment slices (e.g. final_data_ind_Hosp[cols] = ... and final_data_ind_spec['Unique/Common'] = ... on filtered views).
5. Magic-number coupling. hosp_nan=[208546] hardcoded to special-case a "hospital network" — undocumented, fragile.

#### How should we handle failure and correctness posture in the rewrite?

    Fail loud, structured, atomic; fix inline
    Remove warning-suppression + bare excepts, structured logging, mark Failed-with-reason + roll back partial writes + dead-letter on retry-exhaustion. Fix bugs 1–3 as part of the port.

#### You may ask me how do I prove the DuckDB rewrite produces the same numbers as today's pandas code?


    Golden equivalence + unit tests
    Baseline today's outputs on the seeded dummy data (3 SQL payloads + files), assert rewrite matches with a documented list of intentional bug-fix diffs. Unit-test pairwise common/unique, score categorization, client column branching.


## Implementation Notes — provider_pipeline.py

I built the rewrite described above. It's `provider_pipeline.py`, and it replaces `provider_count_assignment.py` + `provider_download_assignment.py` with the Option A design from earlier: one job, one DuckDB connection, two output builders.

#### What it does, end to end

Per request, it opens one DuckDB connection and loads the score CSVs and the base network's data exactly once. Then for each compare network, it loads that network's data, unions it with the base, joins scores, and builds both the count aggregates and the row-level download flags off that same union — so the base network is never re-read, and there's no second full pass for the second output. The score categorization (High/Medium/Low buckets) that used to be a `groupby().apply()` is now a SQL `FILTER` aggregation.

Outputs, written under `./sample_data/output/`:
- `NI+Improved_Results_Table.csv`, `NI+Improved_Results_BarGraph.csv`, `NI+Improved_Results_PerformIndicators.csv` — the count/aggregate tables
- `Individual/<req_id>.{csv,parquet}` and `Organization/<req_id>.{csv,parquet}` — row-level download files, still branching column sets on `clientId` the same way the originals did

#### Scope: this is the compute layer, not the whole job

`provider_pipeline.py` does read → clean → score-join → aggregate → write-files, full stop. It does not write to SQL Server and it does not send email, both of which the production counts script does today. That's intentional, not an oversight — see [ADR 0001](./docs/adr/0001-compute-only-scope-for-pipeline.md). The parameterized/transactional/idempotent SQL write path from the question above still needs to be built and wired in separately before this can fully replace the original counts script in production.

#### A couple of things worth flagging if you're picking this up

- `HOSP_NAN_NETWORKS = {208546}` is carried over from the original on purpose: that one network has no Organization (hospital) data source at all, so the pipeline writes a blank placeholder row for it instead of a hospital count of 0 — reporting an actual zero would read as "this network has no hospitals," which isn't the same thing as "we have no data for it."
- Found and fixed one real bug while porting: the score-cleanup step ran `Quality Score Confidence` (a Green/Yellow/Red category, not a number) through `pd.to_numeric()` before filling blanks, which silently turned every row's confidence value to `"NA"`. Fixed by pulling it out of the numeric-coercion pass. Reran against the seeded dummy data afterward and confirmed real Green/Yellow/Red values now come through instead of 100% `NA`.
- The aggregate CSVs are now named `NI+Improved_Results_*.csv` instead of the originals' `NI+_Results_*.csv` — same three tables, new filenames, worth double-checking nothing downstream is still looking for the old names.
- A post-implementation value-level audit (2026-07-01) found and fixed two issues the row-count-only equivalence check in `PERFORMANCE_BASELINE.md` had missed — see the next section.

## Known output differences from the originals

The "row counts match exactly" equivalence claim in `PERFORMANCE_BASELINE.md` was true, but
only at row-count granularity. A follow-up audit diffed actual cell values and found:

1. **Hospital Utilization Score in the market report (intentional fix, now disclosed)** —
   the original only renames `MA Utilization Score` → `Utilization Score` on the individual
   market frame (`mkt_ind`), not the org one (`mkt_org`). After the two are concatenated,
   every Hospital row has `NaN` there, so `groupby().agg({"Utilization Score": "sum"})`
   silently reports `0.0` for Hospital Utilization Score on every Common Counties row in
   `NI+_Results_PerformIndicators.csv`. `provider_pipeline.py` renames both sides, so the
   same rows show real values (e.g. 72/40/28 instead of 0.0/0.0/0.0 on the sample data).
   This is a deliberate, disclosed bug fix — the rewrite's numbers are correct, the
   original's are silently wrong — but it does mean the two outputs are not byte-identical
   on this column.
2. **Organization/hospital download file had ~50% duplicate rows (real regression, now
   fixed)** — the original calls `drop_duplicates()` immediately after subsetting to the
   final hospital column set (narrowing columns can collapse rows that only differed in a
   dropped field); `provider_pipeline.py` was reindexing to the same column set but skipping
   that dedup call, producing 36 rows where the original produced 24 on the sample data.
   Fixed by adding the same `drop_duplicates()` call back in (see `provider_pipeline.py`,
   the `hosp_df_out`/`ind_only` reindex lines). Verified after the fix: both scripts now
   produce identical row counts and matching content on `Check_1` (sample data), modulo one
   pre-existing, separately-disclosed `"Not Available"` vs `"NA"` string difference (the
   original has a case-mismatch bug — `"Not available"` vs `"Not Available"` — that leaves
   some rows unmatched; the rewrite's cleaner code path always normalizes to `"NA"`).

Net effect: a real cell-by-cell golden diff (the bar `PERFORMANCE_BASELINE.md` itself names
as the right one) is what surfaced both of these — row-count checks alone weren't enough.

## Assumptions

- The provided `sample_data/SAMPLE` tree and `generate_dummy_data.py` output are
  representative in *shape* (file layout, column names/types, the specialty-code exclusion
  list, the `clientId` branching values `[45, 5, 8]`) of production data, even though volumes
  are obviously much smaller. Performance numbers in `PERFORMANCE_BASELINE.md` should be read
  as directional, not as a production capacity estimate.
- `HOSP_NAN_NETWORKS = {208546}` and the `clientId`-based column-set branching are treated as
  real, intentional business rules to preserve as-is, not as legacy cruft to clean up —
  I don't have the business context to know whether other networks/clients need similar
  special-casing, so I carried these forward unchanged rather than generalizing them.
- The brief's description of the production write path (SQL Server INSERT/UPDATE, SMTP
  email) is accurate even though that code isn't present in this repo's scripts (the SQL
  write path is stubbed out in every commit). My recommendations for that path (parameterized,
  transactional, idempotent, pooled, secrets out of source) are therefore design
  recommendations against the brief's narrative, not fixes verified against running code.
- "Counts and downloads are always requested together" is the assumption behind choosing
  Option A (one job, two output builders) over Option B (two jobs, shared materialized
  layer) — see the "How do we handle the two outputs" section above. If download is actually
  an on-demand, separate user action, Option B is the better fit and the architecture
  decision should be revisited.

## What I'd do with more time

- **Golden equivalence test suite.** Bake the cell-level diffing I did manually on
  2026-07-01 into an automated test: run both the originals and `provider_pipeline.py`
  against the seeded dummy data, diff every output file cell-by-cell, and assert equality
  except for a maintained allowlist of intentional, documented diffs (the Utilization Score
  fix above). This is the single highest-leverage next step — it's what would have caught
  both issues above automatically instead of via a manual audit.
- **Unit tests** for the pieces most likely to silently drift: common/unique provider
  classification, score categorization thresholds (High/Medium/Low), and the `clientId`
  column-branching logic, independent of the end-to-end golden diff.
- **Build the persistent worker + result cache** flagged as unchecked in
  `PERFORMANCE_BASELINE.md` — the dominant remaining cost on real production volumes is
  still interpreter/library import time and re-reading reference data per request, not the
  DuckDB business logic itself.
- **Build and wire in the parameterized/transactional/idempotent SQL write path** (and the
  email step) that `provider_pipeline.py` deliberately left out per ADR 0001 — required
  before this can fully replace `provider_count_assignment.py` in production.
- **Deduplicate this write-up.** `README.md` and `docs/adr/solution_approach_map.md` are
  near-duplicates of each other; worth collapsing into one canonical source before this goes
  much further, so corrections like the ones made on 2026-07-01 don't need to be applied
  twice.
- Re-run the import-time profiling with `duckdb` correctly declared in `requirements.txt`
  (added 2026-07-01 — it was missing, so a clean `pip install -r requirements.txt` couldn't
  actually run `provider_pipeline.py`) to get an accurate cold-import baseline.

See [CONTEXT.md](./docs/CONTEXT.md) for the terms used above (base/compare network, common/unique provider, etc.).

