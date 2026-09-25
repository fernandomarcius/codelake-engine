# Install

Two supported layouts: a single node with Docker Compose, or Kubernetes with the
manifests in [`deploy/k8s`](../../deploy/k8s). Both run the release image
(distroless, non-root uid 65532, read-only root file system) pinned **by
digest**. Configuration keys are described in [configuration.md](configuration.md).

## Before you start

1. **Image.** Take the digest from the release (`image-digest.txt` on the
   release page). Optionally verify the keyless signature and provenance:

   ```bash
   cosign verify <image> \
     --certificate-identity-regexp 'https://github.com/<org>/<repo>/.github/workflows/release.yml@.*' \
     --certificate-oidc-issuer https://token.actions.githubusercontent.com
   ```

2. **Configuration.** Start from [`examples/codelake.toml`](../../examples/codelake.toml)
   or [`docs/examples/codelake.production.toml`](../examples/codelake.production.toml).
   Every secret is referenced by the *name* of an environment variable
   (`password_env`, `connection_string_env`, `access_key_id_env`, …).
3. **Secrets file / Secret.** One `NAME=value` line per variable the
   configuration names. On a host: a file owned by the service account, mode
   `0600`, outside any repository. On Kubernetes: a Secret created by the
   secret manager integration (External Secrets, Vault), never committed.
4. **Validate before deploying.** `check-config` reads the file and the
   environment, reports every problem at once and exits `0` (valid) or `2`
   (invalid). It starts nothing and contacts no source.

   ```bash
   docker run --rm --env-file <secrets-file> \
     -v "$PWD/codelake.toml:/etc/codelake/codelake.toml:ro" \
     <image> check-config --config /etc/codelake/codelake.toml
   echo "exit=$?"   # 0 = valid
   ```

   A TLS file named by `server.tls` must exist at the path the container sees;
   mount it for this check too.

## Docker Compose (single node)

[`deploy/compose/codelake.yml`](../../deploy/compose/codelake.yml) runs the
engine alone (the `docker-compose.yml` next to it is the disposable development
stack of sources, not a production file).

1. Prepare a directory on the host, for example `/srv/codelake`:

   ```bash
   sudo install -d -m 0750 /srv/codelake
   sudo install -d -o 65532 -g 65532 -m 0750 /srv/codelake/spill
   sudo install -d -o 65532 -g 65532 -m 0750 /srv/codelake/warehouse   # only with a hive catalog on this host
   sudo install -m 0640 codelake.toml /srv/codelake/codelake.toml
   sudo install -m 0600 codelake.env  /srv/codelake/codelake.env       # secrets
   ```

   In the configuration, the listeners must bind `0.0.0.0` inside the
   container (Docker forwards to the container address) and therefore need
   `allow_unauthenticated_network = true`; the compose file publishes every
   port on the host's `127.0.0.1` only, which is what keeps the unauthenticated
   listeners private (see [configuration.md](configuration.md#listeners-and-security)).
   `memory.spill_dir` must be `/var/lib/codelake/spill` and a `hive`
   warehouse `/data/warehouse` (the mount points of the compose file).

2. Start it:

   ```bash
   export CODELAKE_IMAGE=<image>          # by digest
   export CODELAKE_MEMORY_LIMIT=8g CODELAKE_CPUS=8
   docker compose -f deploy/compose/codelake.yml --project-directory /srv/codelake up -d
   ```

3. Verify:

   ```bash
   docker compose -f deploy/compose/codelake.yml --project-directory /srv/codelake ps
   curl -fsS http://127.0.0.1:8080/livez            # "ok"
   curl -fsS http://127.0.0.1:8080/readyz           # {"ready":true,...}; 503 lists required sources down
   curl -fsS http://127.0.0.1:8080/healthz | jq .   # version, memory ceiling, every source
   docker compose -f deploy/compose/codelake.yml --project-directory /srv/codelake logs --tail 50 codelake
   ```

   The log must show `source attached` once per source and `engine ready`.
   Then run the [canary](canary.md) once.

4. Undo: `docker compose … down` stops and removes the container; the spill
   and warehouse directories stay on the host. Never delete the warehouse
   without a [backup](backup-restore.md).

The image has no shell, so Compose has no in-container health check; probe
`/readyz` from the host (the canary or the monitoring agent).

## Kubernetes

The base manifests: `Deployment` (2 replicas, probes on `/livez` and
`/readyz`, `terminationGracePeriodSeconds` 45 > `shutdown_grace_secs` 30,
spill `emptyDir` with `sizeLimit`), `Service`, `PodDisruptionBudget` and a
`NetworkPolicy` that admits the PostgreSQL port only from namespaces labelled
`codelake.io/client=true` and the ops port only from `monitoring`.

1. Create an overlay per environment instead of editing the base:

   ```text
   overlays/<env>/
     kustomization.yaml
     codelake.toml
   ```

   ```yaml
   # overlays/<env>/kustomization.yaml
   apiVersion: kustomize.config.k8s.io/v1beta1
   kind: Kustomization
   namespace: <ns>
   resources: ["../../deploy/k8s"]
   configMapGenerator:
     - name: codelake-config
       behavior: replace
       files: ["codelake.toml=codelake.toml"]
   images:
     - name: codelake
       newName: <registry>/codelake
       digest: sha256:<digest>
   ```

2. Add what the base does not have, as patches in the overlay:
   - **Client HTTP protocol listener** (`[server.trino]`): a container port,
     a `Service` port and a `NetworkPolicy` rule for it (the base exposes only
     `5432` and `8080`).
   - **Hive warehouse**: a `PersistentVolumeClaim` mounted at the path of
     `warehouse`, writable by uid 65532 (`fsGroup` is already 65532). With
     more than one replica writing the same warehouse the volume must be
     `ReadWriteMany` on a file system with working POSIX advisory locks
     (`flock`); CodeLake's cross-process locks rely on them (ADR 0009). That is
     not verified on network file systems yet: run **one replica** for an
     installation that writes hive tables, or keep writes on one pod.
   - **Resources**: `requests.memory` = `limits.memory`, and `memory.limit`
     in the TOML at about 75% of it ([sizing.md](sizing.md)).
   - Spill `emptyDir.sizeLimit` above `memory.spill_limit`.

3. Label the namespaces of the clients and create the Secret:

   ```bash
   kubectl create namespace <ns>
   kubectl label namespace <client-ns> codelake.io/client=true
   # Secret from the secret manager; by hand only from a 0600 file that is deleted afterwards:
   kubectl -n <ns> create secret generic codelake-sources --from-env-file=<secrets-file>
   ```

4. Render, review and apply:

   ```bash
   kubectl kustomize overlays/<env> > /tmp/codelake-<env>.yaml   # review the diff
   kubectl diff -k overlays/<env>
   kubectl apply -k overlays/<env>
   kubectl -n <ns> rollout status deploy/codelake --timeout=5m
   ```

5. Verify:

   ```bash
   kubectl -n <ns> get pods -l app.kubernetes.io/name=codelake
   kubectl -n <ns> port-forward deploy/codelake 8080:8080 &
   curl -fsS localhost:8080/readyz && curl -fsS localhost:8080/healthz | jq .
   kubectl -n <ns> logs deploy/codelake --tail=50 | jq -r '.fields.message'
   ```

   Then run the [canary](canary.md) from a pod in a client namespace.

6. Undo: `kubectl -n <ns> scale deploy/codelake --replicas=0` stops serving
   and keeps everything else. Deleting the overlay's resources deletes a
   `PersistentVolumeClaim` defined in it: take a [backup](backup-restore.md)
   first.

A required source that cannot be reached at startup stops the process (the pod
restarts until the source answers or `source_startup_timeout_secs` is raised);
see [incidents.md](incidents.md#a-source-is-down).
