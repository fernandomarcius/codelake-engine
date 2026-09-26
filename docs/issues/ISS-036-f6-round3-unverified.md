# ISS-036 — F6 round 3 (cursors) implemented, not verified

- Kind: pending work · Recorded: 2026-09-26 · Status: open
- Branch: `front/f6-fixes`, HEAD `09e99ff`, 9 commits over main `d06b278` + uncommitted ADR 0016/CHANGELOG (saved in `scratchpad/pausa-final/f6-fixes.patch`): suspended FETCH portal reads its rows at the first Execute (was a WRONG-RESULT defect already in main: interleaved FETCH swapped rows), DECLARE parameters bound on the extended protocol, cancel limited to the executing message (WITH HOLD cursors survive), DISCARD ALL in a pipeline, FETCH/MOVE among statements, 63-byte cursor names, tests for held rows/DISCARD portals/ABORT slots, savepoints and AND CHAIN, SHOW/START TRANSACTION tags.
- Not merged: gates of the last commits and the independent verification did not run to the end.
- Risk: main keeps the interleaved-FETCH wrong result for JDBC `setFetchSize` users of the PostgreSQL wire until this lands.
- Next: finish docs from the patch, fast + integration, independent verification, merge.
