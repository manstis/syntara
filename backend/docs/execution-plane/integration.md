# Syntara and Execution Plane integration

AO owns user authentication, authorization, the public API, Temporal workflow
dispatch, and integration records. EP is a separately deployed service that
owns accepted work, cluster and target state, execution attempts, result
persistence, and completion delivery. AO calls EP through its authenticated
versioned HTTP API. AO does not import EP persistence code, connect to the EP
database, or run EP workers in its Temporal processes.

For the first deployment, AO and EP may use the existing PostgreSQL server, but
EP uses a distinct database with EP-owned runtime and migration roles. Neither
service's runtime role can connect to the other service database.

## Work and completion

AO validates a script node, selects its digest-pinned image, persists an
encrypted request binding with the Temporal task token, and submits a versioned
invocation to EP over HTTP. It retries a lost or unavailable submission with
the same request ID and frozen payload. EP's API stores the request in its own
database; EP's worker dispatches it independently.

The cold-start EP backend allocates a one-attempt Kubernetes Job and talks to
the SDK node runtime only over gRPC, tunneled through the authenticated
Kubernetes API port-forward. The gRPC protocol carries invocation data,
progress, errors, stdout/stderr fields, and final output. EP does not create a
workload input Secret, read Pod logs, use `exec`, or connect directly to
Temporal. The Job/Pod lifecycle is an implementation detail of this cold-start
backend; a future worker manager can attach to an existing worker without
changing WorkItem semantics.

EP commits results to a durable outbox and delivers them to AO by authenticated
callback. AO deduplicates callbacks in its inbox and bridges the result to the
original Temporal activity. If a callback is missing, AO can reconcile a
terminal EP result through the status API. An uncertain gRPC outcome becomes
`reconciliation_required`: EP does not rerun that attempt, emits the state to
AO, and AO fails the activity with `WorkloadOutcomeUnknownError` while
preserving EP's WorkItem for operator investigation.

AO cancellation is persisted and delivered to EP by stable request ID. EP
forwards cooperative cancellation over gRPC and only reports confirmed
cancellation after the cold-start worker has stopped. A result received during
the cancellation race wins.

## Integration configuration

AO remains the source of integration configuration. Its durable outbox sends
revisioned desired state to EP over the API. EP reports observed status and
revision; AO presents `pending`, `ready`, or a safe error while the services
converge. The two databases do not participate in a cross-service transaction.
Cluster-management credentials are encrypted in EP's database. Workload
credential mounts remain out of scope until ANSTRAT-2422 establishes the
extension contract.

The two AO-owned status state machines are documented in:

- [Activity binding status progression](activity-binding-status.md)
- [Integration execution-plane status progression](integration-status.md)
- [Combined status progression overview](status-progression.md)

## Combined-service smoke evidence

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
