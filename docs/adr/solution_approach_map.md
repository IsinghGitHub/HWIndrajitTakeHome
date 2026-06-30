# Problem Deffinition

## What the code actually does today (per request):

- Orchestrator spawns provider_count_assignment.py and provider_download_assignment.py as subprocesses, passing 13 sys.argv params (incl. JSON blobs for base/compare).

- Both scripts independently re-read the same per-network parquet (Individual) + CSV (Organization) files and the same score/market CSVs from disk, filter by county, dedupe, merge scores, aggregate.
- Counts script writes 3 tables to SQL Server via executemany; download script writes CSV+parquet to disk.


## The headline problems I can already see (I'll drill into each later): 

subprocess cold-start + re-importing pandas/polars/pyarrow every request; the same reference data re-read on every request and duplicated across the two scripts; joblib imported but never used (reads are sequential); row-wise .apply/groupby().apply() in hot paths; an f-string-formatted SQL UPDATE (injection); hardcoded DB/SMTP creds; bare except: swallowing failures; and a copy-paste bug where the empty-compare branch assigns to base_ind_df.


## My recommended Solution: 

Re-architect the execution model. The subprocess-per-request design is the actual scalability ceiling — we pay interpreter cold-start + library import + full reference-data re-read on every single request, and we pay it twice because two scripts read the same data. Optimizing the pandas inside that model treats the symptom. 

I'd move to a persistent worker/service that keeps reference data warm in memory, run one read+filter+aggregate pass that feeds both the counts and the download outputs, and add a result cache keyed on the request inputs (counts/downloads are deterministic functions of their args). I'd reach for a lazy columnar engine (Polars lazy or DuckDB over the parquet lake) so filtering is pushed down instead of reading whole files into pandas.

## I can suggest better engine runs the heavy read + join + aggregate. I would like to defend it as below.

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

## How do we handle the two outputs (counts vs downloads) that today are two scripts? 

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



## Next question is : How do the results get persisted, now that one job produces both the counts (→ SQL) and the detail files? The current write path has several problems stacked together:

Some of teh red flags identified:

- SQL injection: cursor.execute("UPDATE [...] where RequestID={argument}".format(argument=id_parameter)) — id_parameter comes straight from sys.argv. The 3 INSERTs are parameterized, but this UPDATE is string-formatted.
- No idempotency: it INSERTs, then flips status, with no guard. You just chose a queue + worker pool (Q3) — which means retries. A retried job double-inserts every row, because nothing deletes/dedupes by RequestID first.
- No surrounding transaction: conn.commit() happens mid-stream; a crash between the inserts and the status update leaves a half-written, "in-progress-forever" request.
- Connection hygiene: a fresh pyodbc.connect with inline plaintext credentials per job, Driver={SQL Server} (the legacy driver), and a comment claiming "pool connection" with no pool.


My recommended answer: 

(A) Parameterized writes, wrapped in one transaction, made idempotent, over a pooled connection, with secrets pulled from env/secret-manager. Concretely: parameterize every statement (kill the f-string); make the write idempotent with a DELETE FROM ... WHERE RequestID = ? (or MERGE) before the inserts so a retried job is safe; wrap delete+insert+status-flip in a single transaction so the request row's status is always consistent with the data; reuse a pooled connection from the worker rather than reconnecting per job; and move the connection string + SMTP creds out of source into config/secrets. Volume is modest (counts are aggregated; detail goes to files, not SQL), so fast_executemany is fine — no need for BCP/staging-table machinery (C) unless the counts tables turn out to be huge.


 Parameterized, transactional, idempotent, pooled, secrets out. Now the dimension the brief names first — correctness — because the current code fails silently, and a re-architecture either consciously fixes that or inherits it.



 I also found some concrete latent bugs while reading, not hypotheticals:

1. Dead empty-guard (both scripts). In load_compare_files, the if compare_ind_df.empty: branch assigns to base_ind_df, not compare_ind_df (count script line ~229, download line ~232). Copy-paste from the base loader. The intended empty-frame guard silently never applies to the compare side.
2. NameError on empty compare list. In extract_data, final_data_ind/final_data_org are only bound inside the for compareId loop, then used after it. If compareorgid_list is empty, those names don't exist → crash. The counts script has the same shape.
3. Swallowed failure. extract_data's except builds error_message then... does nothing — no print, no emit_error, no return. So it returns None → main prints "Failed" with zero diagnostics, and any output files already written are left orphaned.
4. Blanket blindness. warnings.filterwarnings("ignore") + bare except: (in send_email) hide real SettingWithCopyWarnings firing on genuine chained-assignment slices (e.g. final_data_ind_Hosp[cols] = ... and final_data_ind_spec['Unique/Common'] = ... on filtered views).
5. Magic-number coupling. hosp_nan=[208546] hardcoded to special-case a "hospital network" — undocumented, fragile.

## How should we handle failure and correctness posture in the rewrite?

    Fail loud, structured, atomic; fix inline
    Remove warning-suppression + bare excepts, structured logging, mark Failed-with-reason + roll back partial writes + dead-letter on retry-exhaustion. Fix bugs 1–3 as part of the port.

## You may ask me how do I prove the DuckDB rewrite produces the same numbers as today's pandas code?


    Golden equivalence + unit tests
    Baseline today's outputs on the seeded dummy data (3 SQL payloads + files), assert rewrite matches with a documented list of intentional bug-fix diffs. Unit-test pairwise common/unique, score categorization, client column branching.


`