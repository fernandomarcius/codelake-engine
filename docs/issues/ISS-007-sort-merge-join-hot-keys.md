# ISS-007 — Sort-merge join with hot keys fails at the ceiling

- Kind: memory limit · Accepted by: Fernando Farias, 2026-09-25 · Status: open
- Context: hash joins over the pool are rewritten as sort-merge joins (ADR 0003). A key repeated many times on the buffered side keeps its key columns in memory while the rows spill, growing outside the pool's checks.
- Accepted: the join observes that growth and fails with a resources error at most one batch of keys above the limit, like the hash join did, instead of skipping the rewrite from distinct-value statistics.
- Also accepted as known limits: joins without statistics still fail cleanly; the rewritten join runs as a single stream (less parallelism); non-equi joins and float/zoned keys stay hash joins.
- Risk: queries with heavily skewed join keys fail instead of completing.
- Revisit when: distinct-value statistics are reliable enough to route skewed joins, or DataFusion's hash join spills.
- References: ADR 0003, `join_spill.rs`, commits db0bf29, ad4c4fa.
