#!/usr/bin/env bash
set -euo pipefail
mkdir -p /test-cache
manager.test -test.v -test.timeout=3m
iris-tc.test -test.v -test.timeout=3m
# The existing Judger requires a writable, delegated cgroup. The Compose verify
# service is privileged and has its own cgroup namespace, as the Iris PoC does.
export CONTAINER_ID="tc-verify-$$"
cg="/sys/fs/cgroup/sandbox-$CONTAINER_ID"
if ! grep -qw memory /sys/fs/cgroup/cgroup.subtree_control; then
  mkdir -p "/sys/fs/cgroup/runner-$CONTAINER_ID"
  # Leave the private namespace root empty before enabling domain controllers.
  echo $$ > "/sys/fs/cgroup/runner-$CONTAINER_ID/cgroup.procs"
  echo +memory > /sys/fs/cgroup/cgroup.subtree_control
fi
mkdir -p "$cg"
echo +memory > "$cg/cgroup.subtree_control"
trap 'rmdir "$cg" 2>/dev/null || true' EXIT
iris-handler.test -test.run 'TestManagerWithRealJudgeAndRun|TestVersionedRequest|TestIncompleteVersion|TestMissingVersion' -test.v -test.timeout=3m
