# TC Manager and Iris PV cache integration

The Manager alone interprets manifests and owns the versioned cache. Iris asks
for a pinned version over a same-node Unix socket, receives read-only input and
expected-output FDs, reads them into its existing `Testcase` representation, and
closes the FDs before judging. No shared Go package or module dependency is used.

## Artifact and socket contracts

The origin stores immutable external `manifest.json` objects and separate
public/hidden `.tar.zst` bundles. This is the PV PoC format, unchanged. SHA-256 is
computed over compressed bundle bytes. Installations are published atomically at
`sha256/<first-two-digest-characters>/<digest>`. Manifests are cached at
`manifests/<sha256(manifestKey)>.json`. Use a separate cache root when changing the
origin endpoint or bucket. The old gzip PoC cache is not migrated; `/cache/v2` is
the new default.

An acquire request is a JSON SOCK_SEQPACKET message:

```json
{"op":"acquire","testcaseSetId":"sum-v1","manifestKey":"testcases/sum-v1/manifest.json","filter":"all"}
```

Filters are `all`, `public`, and `hidden`. Success returns `testcaseSetId` and
ordered `testcases` metadata (`id`, `hidden`, `scoreWeight`). Exactly two FDs per
case follow, input then expected output, in metadata order, in packets of at most
64 descriptors. An FD packet contains the one-byte payload `1`. Empty selections
return an empty metadata array and no FD packets. JSON packets are limited to
32 KiB. Receivers use close-on-exec and reject truncated, incomplete or invalid
responses. Error responses contain `error` and `code` (`invalid`, `unavailable`,
`resource_exhausted`). Versioned errors never trigger legacy fallback.

One Manager owns each cache via an owner flock. Coalesced loads use bundle
checksums as keys. Cold installation is serialized; pending acquires protect all
required bundles. Per-entry read locks exclude detach while complete file sets
are opened. GC renames/detaches entries under the entry lock, then removes their
private tombstones outside entry and global metadata locks. Independent warm
reads and stats need not wait for slow deletion. Iris receives actual FDs via
SCM_RIGHTS; it does not mount the cache PV or pass cache FDs to libjudger.

Admission uses actual filesystem allocation/free space, including staging and
open-but-unlinked files. The cache filesystem must be dedicated (a directory on
an unrelated shared disk makes that disk's usage count toward the watermarks).
Compressed/unpacked size caps are 64/128 MiB, manifest cap 256 KiB, case cap 512,
archive entry cap 4096. At most eight socket requests and sixteen pending loads
are admitted. Iris bounds materialized input/output to 256 MiB per request.
Malformed or missing cached manifests are refetched; a valid version mismatch fails.
Startup cleans incomplete installs and verifies installed-file receipts before
reuse. Installed bundles are not continuously rehashed; later corruption fails
requests and is not automatically repaired. Manager failure is an infrastructure
failure; Iris never directly opens cache paths as a fallback.

## Run and verify

From this directory:

```sh
docker compose up --build -d --wait
docker compose exec -T manager /tc-cache stats
docker compose --profile tools run --build --rm verify
```

Compose seeds immutable PV-format fixtures into Silo as the origin. It keeps the
Manager cache on a dedicated 16 MiB tmpfs retained across Manager container
restarts by `cache-volume`; taking down the whole stack discards that cache.
`seed` writes request references to `/pins/pins.json`; the old `read` CLI is
removed. `seed-local -root DIR` writes the same fixtures to a local origin for
repeatable offline tests.

The integrated verification image builds both modules independently and runs
Manager tests, Iris FD contract tests and real Iris judge/run with the released
Judger executable and C compiler. This tests the request factories, actual build,
execution and output comparison, without requiring PostgreSQL or RabbitMQ.
The self-contained socket tests start a real Manager process with local origin
fixtures; they prove warm zero-origin reads by removing the origin, then restart
the Manager and repeat the request. The integrated verifier also checks the
Compose Silo-backed Manager through `TC_MANAGER_SOCKET`.

`verify` is privileged to match existing Judger cgroup requirements; it delegates
memory control inside its private cgroup namespace. Use an isolated development
Docker host. Unit/FD tests themselves need no privileged execution.

For same-node separate-Pod checks and Pod recreation, see
[the kind integration instructions](../../infra/k8s/iris/poc/local-pv/README.md).
Production backend version publishing/migration and RabbitMQ transport are not
changed by this integration.
