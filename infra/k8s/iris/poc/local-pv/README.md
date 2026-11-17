# Node-local Manager/Iris integration

`integration.yaml` is an isolated test deployment. It runs the actual Iris
judge/run factories, C compiler, Judger and output comparison in one Pod, with a
separate Manager Pod. Iris mounts only the socket; only the Manager mounts the
cache PVC. The seed Job writes immutable PV-format objects to a separate local
origin. These checks do not exercise RabbitMQ or PostgreSQL.

Build images from the repository root:

```sh
docker build -t tc-local-caching:dev apps/tc-local-caching
docker build -t tc-local-caching:integration -f apps/tc-local-caching/integration.Dockerfile .
kind create cluster --name tc-cache-integration --image kindest/node:v1.32.2
kind load docker-image tc-local-caching:dev tc-local-caching:integration --name tc-cache-integration
```

Prepare `/var/lib/tc-cache-integration/cache` on the kind node as a **dedicated
filesystem** before applying. For example, an ext4 loopback filesystem:

```sh
docker exec tc-cache-integration-control-plane bash -ec '
  mkdir -p /var/lib/tc-cache-integration/cache
  truncate -s 128M /var/lib/tc-cache-integration/cache.img
  mkfs.ext4 -q -F /var/lib/tc-cache-integration/cache.img
  mount -o loop /var/lib/tc-cache-integration/cache.img /var/lib/tc-cache-integration/cache
'
kubectl --context kind-tc-cache-integration apply -f infra/k8s/iris/poc/local-pv/integration.yaml
kubectl --context kind-tc-cache-integration -n tc-cache-integration wait --for=condition=complete job/seed --timeout=120s
kubectl --context kind-tc-cache-integration -n tc-cache-integration wait --for=condition=Ready pod/manager --timeout=120s
kubectl --context kind-tc-cache-integration apply -f infra/k8s/iris/poc/local-pv/integration-iris.yaml
kubectl --context kind-tc-cache-integration -n tc-cache-integration logs -f iris
```

The Iris test waits for the Manager and checks both versions, public/hidden
filters, accepted C submissions and wrong answers. Require Pod phase `Succeeded`,
not just Ready. Recreate Iris with the same spec and require the same results.
Rename the node's `objects` directory out of the way and recreate Manager and
Iris (do not rerun the seed Job). Cached requests must still succeed. Manager
`/tc-cache stats` must show no bundle downloads after restart. Restore the origin
before testing a cold version. Delete the dedicated kind cluster when finished.
Do not run the filesystem preparation commands on an existing non-test cache.

`storage.yaml`, `manager-pod.yaml`, and `iris-pod.yaml` adapt the older
`iris-pv-poc` full-service setup to Manager ownership. That setup still requires
its existing development S3, PostgreSQL and RabbitMQ endpoints. Its PV must also
be a dedicated filesystem; adjust Manager watermarks to its actual capacity.
The obsolete direct-PV benchmark Pod has been removed with its loader.
