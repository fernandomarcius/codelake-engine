# ISS-001 — jemalloc as the global allocator

- Kind: dependency · Accepted by: Fernando Farias, 2026-09-24 · Status: open
- Context: the system allocator kept freed pages resident, so RSS stayed far above the pool after large queries (ADR 0003).
- Accepted: `tikv-jemallocator` as the server's global allocator, with page decay set to 0.
- Risk: one more native dependency; allocator behavior differs per platform; RSS numbers are only comparable between builds with the same allocator.
- Revisit when: the pool accounting covers the gaps listed in ADR 0003, or a benchmark shows the decay setting costing more CPU than it saves memory.
- References: ADR 0003.
