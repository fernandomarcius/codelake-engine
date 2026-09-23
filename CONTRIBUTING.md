# Contributing

1. **Open an issue first** using the *Feature (Definition of Ready)* template. Work
   starts when objective, scope, acceptance criteria, scenarios and criticality are
   filled. Missing information goes back to clarification; we do not guess
   business rules.
2. **Short-lived branches** from `main` (hours to a few days). Trunk-based.
3. **Conventional Commits** for commits and PR titles (`feat:`, `fix:`, `perf:`,
   `refactor:`, `test:`, `docs:`, `ci:`, `chore:`).
4. Run the gates locally, in the same container CI uses:

   ```bash
   scripts/dev.sh scripts/gates.sh fast
   ```

   ```bash
   scripts/dev.sh scripts/gates.sh integration
   ```

5. Open a PR with the template checklist. All gates must be green before review.
   Reviews cite objective criteria (requirement, ADR, measurable risk); personal
   preference alone does not block.

## Rules that are easy to miss

- Any planner, pushdown or connector change needs an integration test that asserts
  **rows**. Wrong results are the worst defect a query engine can have.
- New memory-hungry code reserves from the memory pool; nothing buffers unbounded.
- Connectors depend on `codelake-core`, never the reverse (`cargo xtask arch`).
- DataFusion-related crates move together (docs/adr/0002).
- `#[allow]`/`#[expect]` need `reason = "…"`; relaxing a gate needs an entry in
  `docs/exceptions.md` with an expiry date.
