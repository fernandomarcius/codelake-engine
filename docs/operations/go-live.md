# Go-live checklist

Copy this list into the installation's change record and tick each item with
its evidence (a command and its output, a link, a canary line). An item
without evidence is not done.

## Before the cut-over

**Build and release**

- [ ] The image is a release, pinned by digest; its signature verified
      ([install.md](install.md#before-you-start)).
- [ ] Every quality gate green on the released commit (`gates.sh fast` and
      `integration`; CI of the tag).
- [ ] `CHANGELOG.md` read since the version the clients were tested with;
      breaking entries checked against the clients' SQL.

**Configuration and security**

- [ ] `check-config` exits 0 with the production file and secrets.
- [ ] No secret in the TOML, the repository, the ConfigMap or a command line;
      source accounts are read-only where the engine only reads.
- [ ] Query listeners reachable only from the client network (NetworkPolicy,
      loopback publish, or firewall) because there is no client
      authentication yet; ops listener reachable by monitoring only.
- [ ] `memory.limit` ≈ 75% of the container limit; spill on a dedicated
      volume larger than `spill_limit` ([sizing.md](sizing.md)).
- [ ] Client time limits (`query_max_run_time`) set in the pipelines'
      profiles.

**Operations**

- [ ] Metrics scraped; the alerts of [observability.md](observability.md)
      active and routed to the on-call.
- [ ] Logs collected; traces exported if used.
- [ ] Metastore and warehouse backups running; **one restore tested**
      ([backup-restore.md](backup-restore.md)).
- [ ] Compaction jobs scheduled on exactly one engine
      ([compaction.md](compaction.md)).
- [ ] The on-call has read [incidents.md](incidents.md) and can run the
      canary, cancel a query and roll back.

**Evidence of correctness and speed**

- [ ] Shadow run: every output table of the pipelines equal to the reference
      engine's (row count and checksum) over several consecutive days,
      heaviest runs included ([migration.md](migration.md#4-side-by-side)).
- [ ] Peak memory of the heaviest runs measured and within the container.
- [ ] The canary configured: closed-period counts and checksums of every
      critical table, one check per catalog, reference comparisons, freshness
      of every table the pipelines write, latency limits from the objective.
      `--check-config` passes; one manual run is green.
- [ ] The way back rehearsed: clients switched to the previous engine and
      back in a test environment ([migration.md](migration.md#going-back)).

## Cut-over

- [ ] Window agreed; pipelines paused; metastore backup taken.
- [ ] Clients switched; the previous engine kept installed and able to start.
- [ ] First cycles watched: canary green, freshness within the objective, no
      new errors in the pipelines.

## Canary period (production acceptance)

The installation is accepted when, over **14 consecutive days** in
production:

- [ ] **zero wrong results**: no canary `mismatch` and no confirmed
      wrong-result report from users (a `mismatch` explained by a legitimate
      data change is fixed in the configuration and recorded, not counted);
- [ ] **the objectives met**: freshness of the pipelines' tables within the
      objective (for example 5 minutes end to end) and canary latency within
      its limits, apart from incidents with a recorded cause outside the
      engine (a source outage);
- [ ] no OOM kill, no restart without a recorded cause.

Evidence: the canary's JSON lines for the 14 days (or the
`codelake_canary_*` series), with the count of runs, failures by status and
the worst freshness lag. Any wrong result restarts the 14 days after its
cause is fixed.

Sign-off: the installation's owner and the pipelines' owners. The previous
engine is decommissioned only after the sign-off.
