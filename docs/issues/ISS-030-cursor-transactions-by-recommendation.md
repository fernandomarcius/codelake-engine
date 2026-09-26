# ISS-030 — PostgreSQL cursors and transactions: choices applied by recommendation

- Kind: behavior · Applied: 2026-09-26 (F6 re-verification fixes) · Status: awaiting-confirmation
- Context: the F6 re-verification failed the cursors: they survived `COMMIT`/`ROLLBACK`
  holding their slot and memory, `FETCH 0` returned every row left, a `FETCH` the parser
  does not read queued behind its own cursor, and a `FETCH` after a canceled one returned
  nothing. The fixes follow PostgreSQL; where PostgreSQL leaves a choice, or the engine
  cannot do what PostgreSQL does, the recommendation below was applied.
1. **Breaking:** a `DECLARE` without `WITH HOLD` outside a transaction block fails with
   `25P01`, as in PostgreSQL. Before, it declared a cursor that lived until `CLOSE`.
   The alternative, accepting it and closing the cursor at the end of the message, would
   declare a cursor no statement can read. Clients that declare cursors (psycopg2 named
   cursors, psql's `FETCH_COUNT`) already open a transaction first.
2. Protocol portals are closed with their transaction too: a portal suspended by a row
   limit (JDBC's fetch size) ends at `COMMIT`/`ROLLBACK`, and outside a transaction block at
   the `Sync` after its `Execute`, as in PostgreSQL, freeing its slot. A client that kept
   executing such a portal after its `Sync` gets `34000` (PostgreSQL's answer; `pgwire`
   said `26000`).
3. A `WITH HOLD` cursor keeps its statement running (slot held, rows produced as fetched,
   time limit counting from the `DECLARE`) until it is read to the end, closed or stopped.
   PostgreSQL instead materializes its rows at `COMMIT`; doing so here would put the whole
   result in memory outside the pool.
4. `FETCH 0` on a cursor not declared `NO SCROLL` returns its current row, which the
   server remembers; PostgreSQL does the same for plans that can scan backward and fails
   with `55000` for the others. The engine answers it for every plan.
5. Moves backward (`FETCH -1`, `PRIOR`, `BACKWARD n`) fail with `55000` ("cursor can only
   scan forward", PostgreSQL's answer for a `NO SCROLL` cursor; was `42000`).
   `FETCH ABSOLUTE`, `RELATIVE n` (n ≠ 0), `FIRST`, `LAST` and `MOVE` fail with `0A000`:
   not implemented. A `FETCH` the SQL parser does not read inside a message of several
   statements fails with `42601` at once instead of queueing.
6. A cursor whose stream failed (cancel request, `kill_query`, time limit) cannot be read
   again: `25P02` inside the aborted transaction block, `55000` ("portal cannot be run") for
   a `WITH HOLD` cursor outside one, as PostgreSQL marks a failed portal.
7. `DECLARE … BINARY CURSOR` fails with `0A000` (rows are encoded as text at `DECLARE`;
   PostgreSQL sends them in binary). A `DECLARE` sent alone is checked (`25P01`, `42P03`,
   `0A000`) before it takes an admission slot, so a refused one is not listed in
   `system.runtime.queries`.
- Risk: item 1 breaks clients that declared cursors outside a transaction; item 2 breaks
  clients that read a portal after its transaction ended (both fail on PostgreSQL too).
- Revisit when: confirmed or overturned by a human; items 3 and 7 with ISS-029 (cursors
  that keep Arrow batches); item 5 if scrollable cursors are needed.
- References: ADR 0016; `crates/codelake-server/src/pg/cursor.rs`;
  `crates/codelake-server/tests/governance.rs` (`cursors_answer_like_postgresql`).
