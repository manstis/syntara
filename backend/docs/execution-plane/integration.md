# Syntara and Execution Plane command exchange

AO and EP share the independently installable `execution-plane-client` package.
AO owns workflow authorization, Temporal correlation and integration state;
EP owns execution, provider attempts and results. They own separate databases,
journals and encryption keys. AO never imports or installs the EP service.

## Work and completion

The script activity commits a frozen `work.execute` envelope and encrypted
private journal context (Temporal identity, attempt and task token) together. Journal workers deliver commands outside DB
transactions. EP commits receipt and a durable `command.accepted` before HTTP
202. Acceptance is independent of execution and never generates another ACK.

EP commits a terminal observation and its `work.execute.completed` child
atomically. Completion is delivered to AO's shared command ingress at
`/api/execution_plane/v1/commands`; its callback route points back to EP.
AO acknowledges receipt and separately completes the exact Temporal activity.
A retry keeps the original command/payload while a generation fence replaces
its token. Loop iterations have distinct logical activity IDs.

All sends and reads require registered peers, CA-verified client certificates
and short-lived signed assertions. Scoped command/child/resource GET endpoints
support recovery. Normal status reads contain no private payloads. Recovery
returns the original completion envelope only to its intended peer.

An ambiguous provider or Temporal RPC is observed or reported as
`outcome_unknown`; it is not blindly retried. A Temporal completion RPC that
might already have succeeded requires matching history before AO marks the
journal effect handled. Cancellation is a durable `command.cancel`; cancellation
acceptance does not prove a workload has stopped.

## Integration configuration

AO freezes each integration revision, including credentials and project
assignments, into a `cluster.provision` or `cluster.delete` command. The
provision command converges the cluster and default target. The EP also offers
discrete cluster and execution-target registration commands. Only the current
revision changes AO's observed UI status; older revisions are superseded and
deletions leave EP tombstones. Draining may use a disabled credential to stop
existing work, while provisioning enabled resources requires an enabled one.

There is one transport journal per participant. The old integration-sync queue
and completion inbox are retired, as is the activity-binding table. Processed legacy outcomes remain read-only
history. Migrations refuse unresolved old delivery or resource observations;
stop old writers before cutover and reconcile obligations before upgrading.
Commands are the only EP mutation API; no legacy compatibility is provided.

## Development and verification

Use side-by-side checkouts as described in the [backend README](../../README.md).
AO's path dependency points only to `src/execution_plane_client`. Its image
uses that named build context, or a checksum-verified client archive for
hermetic builds. The source lock pins committed bytes; the normal release
workflow must publish the referenced revision and exact archive before remote
CI can fetch it. No client service or additional image is required.

Run the installed EP peer matrix from `backend` after building the EP image:

```bash
EP_BILATERAL_IMAGE=localhost/execution-plane:commands \
  tools/scripts/run-with-testcontainers.sh -- uv run --no-sync pytest \
  tests/integration/execution_plane/test_bilateral_commands.py
```

It uses real participant journals and handlers in separate processes/schemas,
with controlled Temporal/provider effects. Production certificate verification
has separate real-HTTPS tests. The complete verification record is in the EP
repository at `docs/superpowers/plans/2026-10-09-execution-plane-client-validation.md`.

The following smoke results predate the command handshake and do not validate
this protocol. They remain evidence for the unchanged node/provider path.

## Historical pre-handshake smoke evidence

On 6 October 2026, the current AO and EP migration branches were run together
against separate AO and EP databases on one PostgreSQL server and a local Kind
cluster. The enabled script-node workflow completed through the AO API, EP API,
EP worker, Kubernetes Job, SDK gRPC transport, durable EP completion event,
authenticated AO callback, and Temporal activity. The persisted EP result had
`stdout = "gate test\n"`, an empty `stderr`, exit code `0`, and completed
resource cleanup. The EP callback received AO's `202 Accepted` response. The
focused PostgreSQL persistence tests and EP unit suite also passed.

The smoke test found and fixed two integration blockers: the dispatcher Role
needed `get` on `pods/portforward` (the API-server connect request is GET), and
HTTPX skipped loading EP's client certificate when its CA was configured as a
string path. EP now builds an SSL context with the CA and client certificate
loaded together. The Role's narrow permission set was checked: port-forward
access succeeds, while Pod logs and exec remain denied.

A separate [HTTP executor smoke run](https://github.com/syntara-orchestration/syntara-execution-plane/blob/migration/ANSTRAT-1803/docs/kind-demo-runbook.md#http-executor-smoke-test)
submitted the SDK HTTP executor image directly to EP and verified a successful
HTTP response over the Job/gRPC path and completed cleanup. It used the current
first-release WorkItem contract and did not run AO's native `http_request`
activity or the AO-to-EP callback path.

This is evidence for the happy path, not full release qualification. The local
Kind cluster uses kindnet, which does not enforce NetworkPolicies. A separate
Calico Kind attempt could not start because its containerd exhausted available
inotify watchers, and the current OpenShift login was unauthorized. Therefore
actual allowed-versus-blocked network traffic remains unverified and must be
tested on an enforcing CNI, preferably the target OpenShift environment. The
callback outage/restart, cancellation, startup failure, ambiguous Execute,
database-role isolation, and migration-upgrade scenarios in the EP
[Kind demo runbook](https://github.com/syntara-orchestration/syntara-execution-plane/blob/migration/ANSTRAT-1803/docs/kind-demo-runbook.md)
also remain release checks.

The merged monorepo Kind/Konflux harnesses that run Alembic from AO, write EP
tables directly, or connect EP to Temporal are not retained as validation for
this service boundary.
