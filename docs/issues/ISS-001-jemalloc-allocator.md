# ISS-001 — jemalloc as the global allocator

- Kind: dependency · Accepted by: Fernando Farias, 2026-09-24 · Status: closed (2026-10-02: implemented; the decay follows the memory pool since f8e6463)
- Context: the system allocator kept freed pages resident, so RSS stayed far above the pool after large queries (ADR 0003).
- Accepted: `tikv-jemallocator` as the server's global allocator.
- Decay (f8e6463, 2026-09-27; `crates/codelake-server/src/main.rs`, `follow_memory_pressure`): the first setting, page decay 0 (every freed page purged at once), cost 2.6× the time and 2.8× the CPU of a one-second decay on TPC-H SF10. The decay now follows the pool (`codelake_core::pool::on_memory_pressure`): `dirty_decay_ms` is 1000 ms while memory is plentiful and 0 while it is scarce (the pool holds more than an eighth of its limit, until it falls under a twentieth), written to every arena through `tikv-jemalloc-ctl`. The memory-ceiling tests hold with it.
- Risk: one more native dependency; allocator behavior differs per platform; RSS numbers are only comparable between builds with the same allocator.
- Revisit when: the pool accounting covers the gaps listed in ADR 0003, or a benchmark shows the adaptive decay costing more CPU than it saves memory.
- References: ADR 0003; commit f8e6463.
