# ISS-031 — PostgreSQL cursors, second round: choices applied by recommendation

- Kind: behavior · Applied: 2026-09-26 (second F6 re-verification fixes) · Status: awaiting-confirmation
- Context: the second F6 re-verification found a `FETCH` among other statements holding its
  rows outside the pool, a connection's statements queued behind its own cursor's slot,
  `DISCARD ALL` leaving cursors open, cursor names not folded to lower case, and the `FETCH`
  and `MOVE` forms the SQL parser does not read failing with `XX000` on the extended protocol.
  The fixes follow PostgreSQL; where PostgreSQL leaves a choice, or the engine cannot do what
  PostgreSQL does, the recommendation below was applied.
1. **A connection holds one admission slot at most.** While a statement of the connection
   holds one (an open cursor, a portal suspended by a row limit), its other governed statements
   run in it at once, each registered with its own deadline, in the slot's resource group. So
   `max_running_per_user` and the resource groups' limits count connections with work, not
   their open cursors: one connection may keep several cursors open in one slot. It runs one
   statement at a time, and their memory is bounded by the pool. The alternative, refusing at
   once a statement that would queue behind its own connection's slot, keeps the count exact
   but fails what PostgreSQL runs (reading a cursor while querying in the same transaction, a
   query beside a JDBC fetch-size portal).
2. **A `FETCH` among other statements holds its rows in the pool, and fails with `53200` past
   it.** Such a `FETCH` must read its rows before the next statement of the message runs, as
   the upstream handler runs every statement of a message before sending. Streaming them would
   mean running the message's statements one by one and sending each result before the next,
   which the upstream handler does not do. PostgreSQL would send them; here a very large
   `FETCH ALL` among other statements fails (send it alone to stream it), its cursor's
   statement fails with `EXCEEDED_LOCAL_MEMORY_LIMIT` and the cursor cannot be read again
   (`55000`), since the rows it read are gone.
3. **`DISCARD ALL` closes every cursor and portal and resets `statement_timeout` only.** The
   session's other settings (`DateStyle`, `search_path`, the time zone) are not reset, and
   prepared statements are not deallocated: `pgwire` keeps them in a store that cannot be
   cleared. `DISCARD PLANS`, `SEQUENCES` and `TEMP` answer their tags: the engine keeps no
   plans, sequences or temporary tables per connection.
4. **`MOVE` supports what `FETCH` does.** `ABSOLUTE`, `RELATIVE n` (n ≠ 0), `FIRST` and `LAST`
   stay `0A000` for both, and every move backward `55000`, where PostgreSQL answers some of them
   without error at the start of a cursor (`MOVE FIRST`, `MOVE BACKWARD ALL`).
5. A cursor's query is now planned by the server as the session's queries are: operands the
   engine does not combine are refused (ADR 0014) and its rows use the session's format options
   (`datafusion-postgres` planned it unchecked, with default formats). A `DECLARE` of another
   kind or without a name fails with `42601` before it takes a slot.
- Risk: item 1 lets one connection hold several cursors' operators in one slot (bounded by the
  pool); item 2 fails, with `53200`, a large `FETCH` that PostgreSQL would send; item 3 leaves
  session settings a pooled connection's previous client set.
- Revisit when: confirmed or overturned by a human; item 2 if messages come to be run statement
  by statement; item 3 with a session-settings reset; item 4 if scrollable cursors are needed.
- References: ADR 0016; `crates/codelake-server/src/pg.rs`; `crates/codelake-server/src/pg/cursor.rs`;
  `crates/codelake-core/src/governance/mod.rs` (`admit_alongside`);
  `crates/codelake-server/tests/governance.rs` (`cursors_answer_like_postgresql`,
  `fetch_among_other_statements_holds_its_rows_in_the_pool`,
  `transaction_end_closes_cursors_and_portals`).
