# ISS-051 — Containers of char(n) are typed with varchar elements

- Kind: known defect (types only) · Recorded: 2026-09-27 · Status: open
- Context: an array, map or row built from `char(n)` values reports `array(varchar)` / `map(varchar, …)` where the reference reports `array(char(5))` / `map(char(3), bigint)`; `typeof(ARRAY[ch, v3])` answers `array(varchar)`. Values, padding and comparisons are right: stored arrays compared with char literals, `ROW` equality, `contains` and `DISTINCT` over arrays of char match the reference (differential cases `R6-*`).
- Evidence: differential cases `char:V4`, `V9`, `V17`, `V19`, `V54` (outcome `TIPO`), run 2026-09-27 on `front/w1-df54` rebased on main.
- Recommendation: carry the element's declared type through container constructors and aggregates (`ARRAY[…]`, `array_agg`, `histogram`, `map_agg`) so the protocol, `typeof` and CTAS report `char(n)` elements.
- Revisit when: a client depends on the element type of such a container, or with the next char(n) round.
- References: ISS-024, ISS-037.
