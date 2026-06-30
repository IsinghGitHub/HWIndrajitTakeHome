# provider_pipeline.py stays compute-only — no SQL writes, no email

`provider_pipeline.py` replaces the read/clean/score-join/aggregate logic of `provider_count_assignment.py` and `provider_download_assignment.py`, but deliberately does not write to SQL Server or send email, even though the production counts script does both today. We scoped the rewrite to read → clean → join → aggregate → write-files, and left persistence and notification to whatever orchestrator/worker calls it, rather than porting the existing SQL/email code as-is or blocking this rewrite on redesigning the write path at the same time.

**Consequence:** before this can fully replace `provider_count_assignment.py` in production, the parameterized/transactional/idempotent SQL write path discussed in the main README still needs to be built and wired in separately — it isn't part of this file.
