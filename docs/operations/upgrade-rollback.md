# Upgrade and rollback

An upgrade changes one thing: the image digest. Configuration changes go in a
separate step ([configuration.md](configuration.md#change-procedure)), so a
rollback never has to guess which of the two broke the service.

## Before the upgrade

1. Read the `CHANGELOG.md` entries between the running version and the new
   one. Entries marked **Breaking** change answers or errors clients see
   (for example text compared with a number now fails with `TYPE_MISMATCH`);
   check the clients' SQL against them.
2. Record the running state:

   ```bash
   # Kubernetes
   kubectl -n <ns> get deploy codelake -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
   kubectl -n <ns> rollout history deploy/codelake
   # Compose
   docker inspect --format '{{.Config.Image}}' "$(docker compose -f deploy/compose/codelake.yml --project-directory /srv/codelake ps -q codelake)"
   curl -fsS http://<ops>/healthz | jq -r .version
   ```

3. Validate the running configuration with the **new** image
   (`check-config`, see [install.md](install.md#before-you-start)): a new
   version can refuse a key the old one accepted.
4. With a `hive` catalog: [back up the metastore](backup-restore.md#metastore)
   and make sure no `OPTIMIZE` runs during the upgrade.
5. Pick a window outside the pipelines' heavy runs; the canary must be green
   before you start (otherwise you cannot tell what the upgrade broke).

## Upgrade

Kubernetes (rolling, one pod at a time, `maxUnavailable: 0`):

```bash
cd overlays/<env>
kustomize edit set image codelake=<registry>/codelake@sha256:<new-digest>
kubectl diff -k . && kubectl apply -k .
kubectl -n <ns> rollout status deploy/codelake --timeout=10m
```

Compose (the node stops serving for the restart, typically under a second to
ready plus the time to attach sources):

```bash
export CODELAKE_IMAGE=<registry>/codelake@sha256:<new-digest>
docker compose -f deploy/compose/codelake.yml --project-directory /srv/codelake pull
docker compose -f deploy/compose/codelake.yml --project-directory /srv/codelake up -d
```

On SIGTERM the old process fails readiness, stops accepting connections and
gives running queries `shutdown_grace_secs` to finish; queries still running
after that fail and clients retry them.

## Verify

1. `/readyz` 200 and `/healthz` shows the new `version` on every node.
2. Logs since the restart have no `ERROR` lines:
   `kubectl -n <ns> logs deploy/codelake --since=10m | jq -c 'select(.level == "ERROR")'`.
3. Run the canary once by hand, then watch its periodic runs and the
   latency/freshness numbers for at least one full pipeline cycle.
4. Memory: `codelake_memory_reserved_bytes` returns near zero between
   queries, and the container's working set stays below its limit.

## Rollback

Roll back on any wrong result, on a canary failure the upgrade explains, or
on errors the old version did not have. Do not debug in production first.

Kubernetes:

```bash
kubectl -n <ns> rollout undo deploy/codelake            # previous ReplicaSet
kubectl -n <ns> rollout status deploy/codelake --timeout=10m
# then restore the previous digest in the overlay, so the next apply does not undo the rollback
kustomize edit set image codelake=<registry>/codelake@sha256:<old-digest>
```

Compose:

```bash
export CODELAKE_IMAGE=<registry>/codelake@sha256:<old-digest>
docker compose -f deploy/compose/codelake.yml --project-directory /srv/codelake up -d
```

Verify as above (`version` is the old one).

**Data written by the new version.** Hive tables keep the file-metastore
layout and `writerVersion` of the reference engine (ADR 0007), so the previous
CodeLake version and the reference engine read them. If a release note says a
version changed metadata it writes, restore the metastore backup taken before
the upgrade ([backup-restore.md](backup-restore.md#restore)) after rolling back,
and replay the writes made since from the pipelines.
