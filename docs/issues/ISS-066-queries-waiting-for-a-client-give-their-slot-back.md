# ISS-066 — Queries waiting for a client give their slot back: choices applied by recommendation

- Kind: behavior · Applied: 2026-10-07 · Status: awaiting-confirmation
- Context: D4 of the shadow replay of the the reporting application (2026-10-07). The application, killed by a
  deploy, read the first page of 18 queries with large results and never canceled them; their executions
  waited for the gone client holding the user's 16 running slots (ISS-026) until the abandonment sweep
  (300 s). The user's next query waited 295.9 s; another user's ran in 0.07 s. The reference engine 431
  keeps such a query `RUNNING` and counted by its resource group until it is done (`InternalResourceGroup`),
  but production runs it without resource groups. ADR 0016, amendment D4.
- Applied by recommendation (the request asked for the slot to be released; these choices were not
  answered):
  1. **A resumed query takes a running slot again** before it produces more. The request assumed it only
     drains; it does not — the plan computes the rest of the result once the client reads — so without
     re-admission a client reading many queries in turn would have all of them computing beyond
     `max_running_*`. It waits ahead of the queries that never ran, behind the suspended ones that asked
     first, and is never refused by a queue limit nor by `queue_timeout_secs` (its time limits apply).
  2. **New settings, by default on**: `[governance] suspend_after_secs = 1` (how long execution waits for
     its client, slot held, before giving it back) and `max_suspended_queries = 32` per node (beyond it a
     query keeps its slot, as before; `0` turns suspension off). The cap bounds what blocked plans hold
     outside the pool (ADR 0003) to what as many running queries would.
  3. **No new state**: a suspended query stays `RUNNING` in `system.runtime.queries` and the protocol's
     `stats`, as the reference lists a query blocked on its output. The gauge `codelake_queries_suspended`
     and `/healthz` `queries.suspended` show it.
  4. **`[server.trino] client_timeout_secs` keeps its default of 300 s** (the reference's
     `query.client.timeout`): its clients ask for the next page only when the application reads more rows,
     so a shorter default would cancel interactive queries the reference keeps. With suspension an
     abandoned query costs memory, not a slot.
  5. **PostgreSQL wire unchanged** (ISS-026 item 4): a portal whose client stops reading keeps its slot.
- Measured against the binary (`crates/codelake-server/tests/governance.rs`,
  `abandoned_large_results_do_not_freeze_their_user`: 18 abandoned `generate_series` results of one user,
  default limits, `client_timeout_secs = 10` for the measurement; run of 2026-10-07):

  | | suspension off (before) | default (after) |
  |---|---|---|
  | running / queued / suspended | 16 / 2 / 0 | 0 / 0 / 18 |
  | same user's next query | 8.61 s (until the abandonment; 295.9 s on the shadow) | 0.046 s |
  | another user's query | 0.072 s | 0.042 s |
  | pool reserved while abandoned | 19.3 MiB | 21.7 MiB |
  | RSS growth while abandoned | 59.5 MiB | 59.5 MiB (≈ 3.3 MiB per query, either way) |
  | after the abandonment | 18 `ABANDONED_QUERY`, slots and pool at 0 | the same |

- Recommended for the cut-over (production node: Warehouse API ≈ 7 concurrent queries, the reporting application
  bursts of 20–24 as one user at every boot, dbt with 8 threads):

  ```toml
  [governance]
  max_running_queries = 32      # ISS-026; peak demand 7 + 24 + 8 = 39 queues ≤ 7 short boot queries
  max_running_per_user = 16     # ISS-026; the application's burst runs 16 at once, the rest within seconds
  max_queued_queries = 1000     # ISS-026
  max_queued_per_user = 500     # ISS-026
  queue_timeout_secs = 600      # ISS-026
  max_suspended_queries = 64    # two deploys within 5 min (2 × 24) plus the API's slow readers;
                                # ≈ 3.3 MiB RSS each: ≈ 210 MiB worst case, pages included (pool)
  suspend_after_secs = 1

  [server.trino]
  client_timeout_secs = 300     # the reference's query.client.timeout
  max_active_queries = 200      # accepted and unfinished: 32 running + 64 suspended + queued; 100 would
                                # refuse new queries while two abandoned bursts wait for the sweep
  ```

  No resource group is needed for this workload: no user can take more than 16 slots, dbt's 8 threads stay
  under it, and the API's queries are admitted at once beside a full the reporting application (D4: 0.042 s).
  Watch `codelake_queries_suspended`: at the cap, abandoned queries hold slots again until the sweep.
- Revisit when: a human confirms or overturns these choices; a client is seen waiting for a slot after
  reading slowly (raise `suspend_after_secs`); `codelake_queries_suspended` reaches the cap; the PostgreSQL
  wire shows the same pattern.
- References: ADR 0016 (amendment D4); `crates/codelake-core/src/governance/{admission,slot}.rs`;
  `crates/codelake-server/src/trino/statement.rs` (`run`); shadow report
  `replay-financeiro/RELATORIO.md` on the shadow host (`govern-candidate.log`).
