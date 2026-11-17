# Integration verification

Date: 2026-11-17. Integration baseline: Manager `3eba4e0f`, Iris `c865c35c`; subsequent cache-file guards and test cleanup are in `cabeedee`.
These are PoC results, not a production rollout or performance claim.

## Executed checks

- Documentation: ADR 0008 and the superseded 0003/0005 entries passed the full
  VitePress build.
- Linux Go 1.26.4: Manager artifact, manifest, checksum, retry, filtering,
  cross-version bundle reuse, restart and GC tests passed. Final source passed
  race detection, including manifest recovery and dedicated 16 MiB tmpfs
  last-FD-close allocation recovery. The per-entry locking/stress suite also
  passed ten repeated race runs before the final manifest-recovery addition.
- The complete Iris Go test suite passed Linux `go test -race -count=1 ./...`
  after synchronizing stub-server shutdown with test-file cleanup; Linux-targeted `go vet`,
  gofmt, diff checks and the repository toolchain-pin check passed.
- The actual Manager process and Iris client passed socket contract tests:
  metadata/order/filter checks, split 260-FD responses, close-on-exec,
  incomplete/truncated responses, descriptor exhaustion, cancellation and
  cleanup. Removing the local origin proved warm reads and Manager restart
  reuse without origin access.
- Compose with a real Silo origin, seeded immutable artifacts, a separate Manager
  container and Iris verification container passed the actual judging tests.
- The actual Iris judge/run factories compiled and executed C using Judger
  v1.0.0-alpha.4. Both fixture versions, public/hidden filters, accepted answers
  and deliberately wrong answers passed. Existing versioned/legacy/custom
  routing and no-fallback tests passed.
- Kubernetes v1.32.2 in a dedicated kind cluster: separate same-node Manager and
  Iris Pods passed real C judging through a Unix socket. Only Manager mounted
  the local PV, backed by a dedicated 128 MiB ext4 loopback filesystem.
- After removing the origin directory and recreating **both** Manager and Iris
  Pods, all actual judging cases passed again. The restarted Manager reported
  the same ready manifests/bundles and an empty download counter.

## Reproduction and limits

See README.md and `infra/k8s/iris/poc/local-pv/README.md`. Local deployment tests
used images packaged from Linux binaries cross-compiled from the final source;
Manager race tests ran in Linux directly. This avoids repeatedly compiling the
AWS SDK inside the emulated development Docker VM. The source-build Dockerfiles
and CI workflow are also supplied. A duplicate source-image build was stopped
after its snapshot became outdated during implementation; its full clean build
was not used as evidence. The final guard-only change was covered by Manager
race tests; deployment checks used the preceding integration baseline.

These tests exercise the actual Iris factories, compiler, sandbox and verdict
path, rather than RabbitMQ/PostgreSQL transport or the complete Iris startup
sequence. That startup retains its legacy S3/database initialization. Node loss,
production workload limits and production rollout are not established here.
