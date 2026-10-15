# ISS-067 — A SQL Server read whose connection is lost mid-stream is not resumable

- Kind: limitation · Applied by recommendation, 2026-10-15 · Status: awaiting-confirmation
- Context: the shadow run (candidate `b73643f`) lost one cycle in about 2200: the dbt model
  `pesendereco_b13`, a `CREATE TABLE AS` over ~961 000 SQL Server rows, ended with
  the driver's io error `… performing I/O: Connection reset by peer (os error 104)` (a reset from the
  SQL Server side; its error log and the container had nothing at that time). The model takes 5.2-5.4 s; the failure came at 4.5 s, in the middle of the stream, after
  several batches (8192 rows each) had reached the consumer. The engine logged nothing.
- Decision: the connector retries a read **only when no batch has reached the consumer**: a lost connection
  (`ConnectionReset`, `ConnectionAborted`, `BrokenPipe` or `UnexpectedEof` from the driver) starts the
  read again on a new connection, up to two more times, after 200 ms and 1 s. Once a batch has been
  handed over the error surfaces as before (same type, same text, same client code). Every lost connection
  is logged at `WARN`, and the span records `codelake.retries`.
- Effect: a connection lost **inside** the stream, as the shadow run's was, is still a failed query. The
  reference engine, run without fault-tolerant execution as in the shadow run, behaves the same: the rows of a federated read are not resumable, and a reader that
  started over would hand the consumer the first rows twice, or end short with no error. The retry covers
  what can be repeated without a trace: the connection dropped while the statement is sent, before the
  first row, or inside the first batch (8192 rows).
- Not done, deliberately: resuming from the last key (needs an ordered, unique key the SQL does not
  have, and a snapshot the second read would not share), buffering the whole result to retry it (breaks
  the memory ceiling), retrying the statement the client sent (the client decides that).
- Risk: a long read through an unstable link (a tunnel, a SQL Server restarting) fails whole every time
  the link drops. If the shadow run's count of `sqlserver connection lost` warnings with
  `batches_delivered > 0` is not near zero, the cause is the link or the source, not the engine.
- Revisit when: the warnings show a recurring loss mid-stream, or a client cannot retry the statement.
  The option then is a bounded, spillable buffer of the first N batches, so a loss inside it stays
  retryable, at a stated memory cost.
- References: `crates/codelake-connectors/src/sqlserver/executor.rs` (module documentation, `next_step`),
  `crates/codelake-connectors/tests/integration/sqlserver_connection_lost.rs`; CHANGELOG, Fixed.
