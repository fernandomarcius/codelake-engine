# ISS-014 — W1 corrections: choices applied by recommendation

- Kind: behavior · Applied: 2026-09-25 · Status: awaiting-confirmation (no explicit human answer; each is reversible)
1. A NANOSECONDS hive catalog fails, naming the value and column, on an `INT96` instant outside 1677-09-21 … 2262-04-11 (what Arrow nanoseconds hold); the reference engine answers it. Before, it answered another instant (W1-2). The alternative, answering those values, needs a timestamp type wider than Arrow's.
2. Tables of the in-memory catalog (`CREATE TABLE datafusion.…`) stay allowed, with their rows reserved in the memory pool; the red-team's first hint was to refuse that DDL, which the reference engine does not have. They remain visible to every user until F5 (R11). A client can fill the pool with them: other queries then fail at the ceiling instead of the server running out of memory.
3. Every comparison of a `char(n)` value is refused (breaking), not only those pushed to a hive scan, until the engine compares char values padded (W1-4, with ISS-010 item 6).
4. A NANOSECONDS scan reads the files twice (microseconds and nanoseconds) and each read keeps its files in their partition and order, so no idle partition takes a sibling's files: slower than one read, never another instant.
- Revisit when: confirmed or overturned by a human; item 1 when the engine gets a wider timestamp; item 2 with F5.
- References: ADR 0007 ("Red-team W1 corrections"), ADR 0009; commits 2ccc9a7, 506aeb4, 91b7ee4.
