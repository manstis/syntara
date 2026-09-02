# Execution Plane

Design document for the Execution Plane — the subsystem that routes, provisions, and dispatches workflow task node workloads to isolated compute workers.

Reference: [ANSTRAT-1803](https://redhat.atlassian.net/browse/ANSTRAT-1803) — Workflow Automation: Execution Plane (MVP: On-Cluster OpenShift).

## Logical Components

The Execution Plane comprises the following logical components:

```mermaid
graph TB
    subgraph ControlPlane["Control Plane"]
        WE["Workflow Engine"]
        TE["Task Executor"]
        R["Reconciler"]
        P["Provisioner"]
        D["Dispatcher"]
        CP["Credential Provider"]
        IP["Isolation Policy"]
        RM["Resource Monitor"]
        EER["Execution Environment Registry"]
    end

    subgraph EP1["Worker Pool A (e.g. On-Cluster OpenShift)"]
        LM1["Lifecycle Manager"]
        W1["Worker"]
        W2["Worker"]
    end

    subgraph EP2["Worker Pool B (e.g. Remote OpenShift Cluster)"]
        LM2["Lifecycle Manager"]
        W3["Worker"]
    end

    WE -->|"execute_task(task definition)"| TE
    TE -->|"resolve pool"| R
    RM -->|"capacity + health"| R
    R -->|"ReconcileResult"| TE
    TE -->|"acquire worker"| P
    TE -.->|"request scale-up (back-pressure)"| P
    EER -->|"container image"| P
    IP -->|"isolation constraints"| P
    P -->|"WorkerHandle"| TE
    TE -->|"dispatch task"| D
    CP -->|"credentials"| D
    D -->|"run task"| W1
    D -->|"run task"| W3
    TE -.->|"task result"| WE
    LM1 --- W1
    LM1 --- W2
    LM2 --- W3
    RM -.->|"health probes"| LM1
    RM -.->|"health probes"| LM2
```

**Simplified view — critical path only:**

```mermaid
graph LR
    WE["Workflow Engine"] -->|"execute_task"| TE["Task Executor"]
    TE -->|"resolve pool"| R["Reconciler"]
    R -->|"read pools"| PR["Pool Registry"]
    TE -->|"acquire worker"| P["Provisioner"]
    TE -->|"dispatch task"| D["Dispatcher"]
    D -->|"inject credentials"| CP["Credential Provider"]
    D -->|"run task"| W["Worker Pool"]
    TE -.->|"task result"| WE
```

### Task Executor

The single entry point into the Execution Plane. The Workflow Engine calls `execute_task(task_definition)` and receives a result — it has no knowledge of pools, provisioning, or dispatch internals. The Task Executor owns the full orchestration pipeline: Reconciler, Provisioner, Dispatcher, and cleanup.

**Responsibilities:**
- Accept a task definition from the Workflow Engine (task payload, affinity labels, workload type, connectivity requirements, EE reference)
- Orchestrate the execution pipeline: reconcile a pool, provision a worker, dispatch the task, collect the result
- Handle provisioning failures by retrying with fallback pools from the Reconciler's ranked list
- **Handle back-pressure:** when the Reconciler returns `CAPACITY_EXHAUSTED`, queue the task and attempt corrective action (see [Back-Pressure and Capacity Management](#back-pressure-and-capacity-management))
- Return the task result (or a structured error) to the Workflow Engine
- Ensure worker cleanup occurs regardless of task outcome (delegates to the Lifecycle Manager)

**Back-pressure handling:**

The Task Executor is the decision-maker for back-pressure. When the Reconciler reports that matching pools exist but have no available capacity, the Task Executor takes corrective action:

| Reconciler outcome | Task Executor action |
|---|---|
| `MATCHED` | Proceed immediately — acquire worker from the best available pool |
| `CAPACITY_EXHAUSTED` | Queue the task, request pool scale-up via the Provisioner, re-poll until capacity frees or timeout expires |
| `NO_MATCHING_POOLS` | Fail immediately with a structured error — no corrective action possible |

The scale-up request is delegated to the Provisioner, which knows how to add capacity to a given pool (e.g. scaling a Deployment's replica count for vanilla K8s pools). The Provisioner may decline the request if the pool is already at its configured maximum.

**Interface:**
- `execute_task(task_definition) -> task_result` — the only method the Workflow Engine calls
- The task definition includes: task payload (inputs, parameters), affinity labels, workload type (action/agentic), connectivity requirements, EE image reference, credential references, resource requirements (CPU, memory, timeout)

### Worker

The smallest granular compute unit. A Worker executes a single workflow task node within a sandboxed container. Each Worker is ephemeral — created for a task, destroyed after completion.

**Responsibilities:**
- Execute the dispatched task within its Execution Environment container
- Report task status (running, completed, failed) back to the Dispatcher
- Enforce resource limits (CPU, memory, timeout) as defined by its configuration

**Key attributes:**
- Compute requirements (CPU, memory, storage)
- Assigned Execution Environment (container image)
- Applied Isolation Policy
- Injected credentials
- Lifecycle state (pending, running, succeeded, failed, terminated)

### Worker Pool

A logical grouping of Workers representing a deployment target — for example, an OpenShift cluster or namespace. A Worker Pool is the unit at which administrators configure capacity, connectivity, and placement policy. It is the singular logical Execution Plane in which Workers can be provisioned.

**Responsibilities:**
- Define the boundary within which Workers are provisioned
- Advertise capacity, connectivity metadata, and health to the Resource Monitor
- Support label-based selection by the Reconciler

**Key attributes:**
- Labels (key/value pairs for affinity matching)
- Connectivity metadata (reachable network endpoints, segments, geographies)
- Capacity limits (max concurrent Workers, resource quotas)
- Provisioner type (which Provisioner implementation manages this pool)
- Status (online, degraded, offline)

### Reconciler

Selects a Worker Pool in which to provision a Worker for a given task node. The Reconciler evaluates task requirements against available Worker Pools, considering labels, connectivity, capacity, and isolation constraints. It returns a structured result that tells the Task Executor both what matched and why anything didn't — enabling the Task Executor to take appropriate corrective action.

**Responsibilities:**
- Match task node affinity labels against Worker Pool labels
- Evaluate connectivity requirements (can the pool reach the endpoints this task needs?)
- Consult the Resource Monitor for available capacity
- Apply Isolation Policy constraints (e.g. agentic workloads excluded from certain pools)
- Return a structured `ReconcileResult` (not just a list) that classifies pools by outcome

**Inputs:**
- Task node label selectors and affinity rules
- Task node connectivity requirements
- Workload type (deterministic action vs. agentic)
- Resource Monitor data (capacity, health, connectivity metadata)

**Structured result:**

The Reconciler returns a `ReconcileResult` that partitions pools into three categories and provides an overall outcome. This gives the Task Executor enough information to decide whether to proceed, queue, scale, or fail.

```python
class ReconcileResult:
    available_pools: list[PoolWithCapacity]    # labels match, capacity available — can run now
    exhausted_pools: list[PoolAtCapacity]      # labels match, but all workers busy
    ineligible_pools: list[PoolIneligible]     # labels/isolation/connectivity mismatch (with reason)
    outcome: ResolveOutcome  # MATCHED | CAPACITY_EXHAUSTED | NO_MATCHING_POOLS
```

| Outcome | Meaning | Task Executor action |
|---|---|---|
| `MATCHED` | At least one pool has capacity | Proceed with provisioning |
| `CAPACITY_EXHAUSTED` | Pools match but all are full | Queue the task, attempt to scale a pool via the Provisioner |
| `NO_MATCHING_POOLS` | No pool matches labels/isolation/connectivity | Fail immediately — scaling won't help |

### Provisioner

Creates a Worker within the selected Worker Pool. The Provisioner is an abstraction over the underlying infrastructure — different implementations handle different pool types (OpenShift pods, OpenShell sandboxes, Agent Sandbox, etc.).

**Responsibilities:**
- Create the Worker compute resource in the target pool (or acquire one from a shared pool)
- Pull and apply the specified Execution Environment container image
- Apply Isolation Policy constraints (namespace isolation, network policies, seccomp profiles)
- Configure resource limits per the Worker definition
- Report provisioning status (success, failure, timeout)
- **Scale-up on demand:** when the Task Executor requests additional capacity for a pool, increase the pool size (e.g. scale a Deployment's replica count). The Provisioner enforces the pool's configured maximum — if the pool is already at max, the scale-up request is declined and the Task Executor must wait for existing workers to free up or time out.

**Key design constraint:** The Provisioner interface is implementation-agnostic. New provisioner types (e.g. OpenShell, RHEL execution nodes) can be added without rearchitecting the interface.

### Dispatcher

Delivers the workflow task payload to a provisioned Worker and manages the task execution lifecycle.

**Responsibilities:**
- Inject credentials into the Worker via the Credential Provider
- Deliver the task payload (inputs, parameters, Execution Environment configuration)
- Monitor task execution (heartbeats, timeouts)
- Collect task output and return it to the Task Executor
- Handle task failure, retry, and cancellation signals

### Execution Environment (EE) Registry

Manages the catalogue of container images available for task execution. An Execution Environment defines the software stack (Python packages, Ansible collections, system libraries) that a task node runs within.

**Responsibilities:**
- Maintain a catalogue of available EE images and their metadata
- Validate that EE images are sourced from OCI-compliant registries accessible from the target Worker Pool
- Provide the Provisioner with the image reference to pull when creating a Worker
- Support a minimum-footprint base image for building custom EEs

**Key attributes per EE:**
- OCI image reference (registry/repository:tag)
- Supported workload types (action, agentic, both)
- Required capabilities or system libraries
- Build lineage (base image, build tool version)

### Resource Monitor

Observes and reports the state of all Worker Pools and their Workers. Provides the data layer that feeds the Reconciler's placement decisions and the administrator's topology view.

**Responsibilities:**
- Probe Worker Pool health (liveness, readiness)
- Track per-pool resource availability (compute capacity, active workload count)
- Collect connectivity metadata (what endpoints each pool can reach)
- Expose metrics for administrator dashboards and topology views
- Detect and report degraded or offline pools

### Credential Provider

Securely injects credentials into Workers at dispatch time. Enforces workload-type isolation — agentic workers must not receive automation credentials, and vice versa.

**Responsibilities:**
- Resolve which credentials a task node requires based on its configuration
- Enforce credential access policies based on workload type and Isolation Policy
- Inject credentials into the Worker's environment securely (not persisted to disk, not logged)
- Support credential rotation without Worker restart

### Isolation Policy

Defines the security and governance boundaries applied to Workers based on workload type. Separates deterministic action workloads from agentic AI workloads to reflect their distinct trust requirements.

**Responsibilities:**
- Classify workloads by type (deterministic action vs. agentic AI)
- Define isolation level per workload type (namespace isolation, network policies, resource constraints)
- Restrict credential access by workload type
- Inform the Provisioner of required container security context (seccomp, AppArmor, capabilities)
- Provide the Reconciler with pool eligibility constraints

**MVP scope:** Container isolation + namespace separation within OpenShift. The interface accommodates future policy-based sandboxing (e.g. OpenShell) without rearchitecting.

### Lifecycle Manager

Manages Workers from creation to cleanup within a Worker Pool. Operates as a per-pool component.

**Responsibilities:**
- Track all Workers in the pool (pending, running, completed, failed)
- Enforce pool-level capacity limits (reject provisioning requests when at capacity)
- Reclaim resources from completed or failed Workers (container cleanup, volume teardown)
- Handle Worker failures (detect unresponsive Workers, mark as failed, trigger re-provisioning if policy allows)
- Report pool state to the Resource Monitor

## Worker Pool Registration

The sections above describe **runtime** behaviour — how the Task Executor selects a registered pool, provisions a worker, and dispatches a task. This section covers the **registration** lifecycle: how a new Worker Pool is introduced to the Execution Plane, bootstrapped on its target infrastructure, and made available to the Reconciler.

Registration is an administrator action, performed once per cluster. It is separate from, and prerequisite to, runtime task execution.

### Registration Model

```mermaid
graph TB
    Admin["Administrator"]
    RP["Registration Provider"]
    CB["Cluster Bootstrapper"]
    PR["Pool Registry"]
    R["Reconciler"]

    Admin -->|"register pool<br/>(type, platform, endpoint, labels)"| RP
    RP -->|"bootstrap cluster"| CB
    CB -->|"configure infrastructure"| Cluster["Target Cluster"]
    RP -->|"store registration"| PR
    PR -->|"registered pools"| R

    style PR fill:#e8f4fd
    style R fill:#e8f4fd
```

### Registration Provider

Accepts a pool registration request from an administrator and orchestrates the registration lifecycle: validate the request, bootstrap the target infrastructure, and store the registration in the Pool Registry.

**Responsibilities:**
- Validate the registration request (endpoint reachability, credentials, platform compatibility)
- Delegate infrastructure bootstrapping to the Cluster Bootstrapper
- Store the completed registration in the Pool Registry
- Support updating an existing registration (e.g. changing labels, resizing capacity)
- Support deregistering a pool (drain workers, tear down bootstrapped resources, remove from registry)

**Registration request includes:**
- Pool name and description
- Cluster endpoint (API server URL, kubeconfig, or connection details)
- Platform type (OpenShift, RHEL)
- Worker type and provisioner backend (see below)
- Labels (key/value pairs for affinity matching)
- Connectivity metadata (reachable network endpoints)
- Capacity limits (max concurrent workers, resource quotas)
- Credentials for cluster access (service account token, certificate, SSH key)

### Worker Type

Each pool is registered with a worker type that determines how workers are provisioned and whether they persist between tasks.

| Worker Type | Lifecycle | Provisioner Model | Description |
|---|---|---|---|
| **Short-lived** | Ephemeral — created per task, destroyed after | Create/destroy | A new worker is provisioned for each task and torn down on completion. No state carries between tasks. |
| **Long-lived** | Persistent — claimed per task, released after | Claim/release (shared pool) | A pool of pre-provisioned workers is maintained. Workers are claimed for a task and returned to the pool on completion. |

Worker type is orthogonal to provisioner backend — for example, a vanilla K8s pool can operate in either mode (Deployment with Lease claiming for long-lived, or pod-per-task for short-lived).

### Provisioner Backend

Each pool is registered with a provisioner backend that determines the runtime technology used to create and manage workers.

| Backend | Short-lived | Long-lived | Description |
|---|---|---|---|
| **Vanilla Kubernetes** | Yes | Yes (MVP) | Standard Kubernetes pods. Long-lived mode uses a Deployment with Lease-based claiming. |
| **OpenShell** | Yes | No | NVIDIA policy-based sandboxing. Sandbox created per task, destroyed after. |
| **Agent Sandbox** | Yes | Yes | K8s-native CRD-managed pools. Warm pool mode pre-provisions pods. |
| **Substrate** | Yes | No | Custom substrate runtime. Sandbox created per task. |

### Platform

Each pool is registered against a target platform that determines the infrastructure capabilities and bootstrapping steps.

| Platform | MVP | Description |
|---|---|---|
| **OpenShift** | Yes | On-cluster or remote OpenShift cluster. Supports namespaces, SCCs, Routes, NetworkPolicies. |
| **RHEL** | No (future) | Standalone RHEL host. Supports Podman-based container execution. Delivered via [ANSTRAT-2338](https://redhat.atlassian.net/browse/ANSTRAT-2338). |

### Cluster Bootstrapper

Configures the target infrastructure to support worker provisioning. What the bootstrapper does depends on the platform and provisioner backend combination. The bootstrapper is an abstraction — each combination has its own implementation.

**Responsibilities:**
- Create or validate the target namespace on the cluster
- Deploy RBAC resources (ServiceAccount, Role, RoleBinding) for the scheduler/provisioner
- Install and configure the provisioner backend runtime (if required)
- Deploy worker Deployment/ReplicaSet (for long-lived vanilla K8s pools)
- Configure network policies and security context constraints
- Validate that the cluster is ready to accept workers
- Report bootstrap status (success, partial, failed)

**Bootstrapping by backend:**

| Backend | Platform | Bootstrap Actions |
|---|---|---|
| Vanilla K8s (long-lived) | OpenShift | Create namespace, deploy RBAC, deploy worker Deployment, configure readiness/liveness probes |
| Vanilla K8s (short-lived) | OpenShift | Create namespace, deploy RBAC, configure pod security context |
| OpenShell | OpenShift | Create namespace, deploy RBAC, install OpenShell operator (Helm), configure SCCs, deploy compute driver |
| Agent Sandbox | OpenShift | Create namespace, deploy RBAC, install Agent Sandbox operator, create SandboxWarmPool CRD |
| Vanilla K8s | RHEL | Configure Podman runtime, deploy agent binary, establish connectivity to control plane |

### Pool Registry

Stores all registered Worker Pool configurations and makes them available to the Reconciler at runtime.

**Responsibilities:**
- Persist pool registrations (database-backed)
- Provide the Reconciler with the current set of registered and healthy pools
- Track registration status (registering, bootstrapping, active, degraded, deregistering)
- Store the pool's provisioner backend type so the Task Executor can select the correct `ProvisionerBackend` implementation at runtime

### Registration Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Registering: Administrator submits registration
    Registering --> Validating: Registration Provider validates request
    Validating --> Bootstrapping: Validation passed
    Validating --> Failed: Validation failed (unreachable endpoint, bad credentials)
    Bootstrapping --> Active: Cluster Bootstrapper completes successfully
    Bootstrapping --> Failed: Bootstrap failed (RBAC error, operator install failed)
    Active --> Degraded: Resource Monitor detects health issues
    Degraded --> Active: Health restored
    Active --> Deregistering: Administrator requests deregistration
    Degraded --> Deregistering: Administrator requests deregistration
    Deregistering --> [*]: Workers drained, resources torn down, registration removed
    Failed --> [*]: Administrator acknowledges or retries
```

### Registration Sequence

```mermaid
sequenceDiagram
    participant Admin as Administrator
    participant RP as Registration Provider
    participant CB as Cluster Bootstrapper
    participant PR as Pool Registry
    participant RM as Resource Monitor
    participant R as Reconciler

    Admin->>RP: register pool (name, endpoint, platform: OpenShift,<br/>backend: vanilla-k8s, worker type: long-lived,<br/>labels: {region: eu-west, gpu: true})

    RP->>RP: validate endpoint reachability
    RP->>RP: validate cluster credentials

    RP->>PR: create registration (status: bootstrapping)

    RP->>CB: bootstrap cluster (platform: OpenShift, backend: vanilla-k8s)
    CB->>CB: create namespace "agent-system"
    CB->>CB: deploy RBAC (ServiceAccount, Role, RoleBinding)
    CB->>CB: deploy worker Deployment (N replicas)
    CB->>CB: validate pods reach Ready state
    CB-->>RP: bootstrap complete

    RP->>PR: update registration (status: active)

    Note over PR: Pool now visible to Reconciler

    RM->>PR: poll registered pools
    RM->>RM: begin health probes for new pool

    R->>PR: query available pools for task
    PR-->>R: [..., {name: "eu-west-gpu", labels: {region: eu-west, gpu: true}, status: active}]
```

### Deregistration

Removing a pool is a controlled process: drain active workers, tear down bootstrapped resources, and remove the registration.

```mermaid
sequenceDiagram
    participant Admin as Administrator
    participant RP as Registration Provider
    participant CB as Cluster Bootstrapper
    participant PR as Pool Registry
    participant R as Reconciler

    Admin->>RP: deregister pool "eu-west-gpu"

    RP->>PR: update registration (status: deregistering)

    Note over R: Reconciler stops selecting this pool<br/>for new tasks

    RP->>RP: wait for active workers to complete (drain)

    RP->>CB: tear down cluster resources
    CB->>CB: delete worker Deployment
    CB->>CB: delete RBAC resources
    CB->>CB: delete namespace (if owned)
    CB-->>RP: teardown complete

    RP->>PR: remove registration

    Note over PR: Pool no longer exists in registry
```

## Sequence Diagrams

### Task Execution: Happy Path

The complete flow from a workflow task node becoming ready to execute, through worker selection, provisioning, and dispatch, to result delivery. The Workflow Engine calls `execute_task` on the Task Executor and receives a result — it has no visibility into the internal pipeline.

```mermaid
sequenceDiagram
    participant WE as Workflow Engine
    participant TE as Task Executor
    participant R as Reconciler
    participant RM as Resource Monitor
    participant IP as Isolation Policy
    participant P as Provisioner
    participant EER as EE Registry
    participant CP as Credential Provider
    participant D as Dispatcher
    participant LM as Lifecycle Manager
    participant W as Worker

    WE->>TE: execute_task(task definition)

    Note over TE: Task definition includes:<br/>payload, affinity labels, workload type,<br/>connectivity requirements, EE reference

    TE->>R: resolve pool (labels, workload type, connectivity)

    R->>IP: get constraints for workload type
    IP-->>R: isolation constraints, eligible pool criteria

    R->>RM: get available pools (capacity, health, connectivity)
    RM-->>R: pool status list

    Note over R: Match task labels + connectivity<br/>+ isolation constraints<br/>against pool labels + metadata.<br/>Rank eligible pools.

    R-->>TE: ranked pool list [Pool A, Pool B]

    TE->>P: provision worker in Pool A

    P->>EER: resolve EE image for task
    EER-->>P: OCI image reference

    P->>IP: get security context for workload type
    IP-->>P: namespace, network policy, seccomp profile

    P->>LM: create worker (image, resources, security context)
    LM-->>P: worker created, worker ID

    P-->>TE: worker provisioned (worker ID)

    TE->>D: dispatch task to worker

    D->>CP: resolve credentials for task node
    CP->>IP: validate credential access for workload type
    IP-->>CP: access granted
    CP-->>D: credentials

    D->>W: deliver task payload + credentials

    W-->>D: task running (heartbeats)
    W-->>D: task completed (output)

    D-->>TE: task result

    TE->>LM: release worker
    LM->>W: terminate + cleanup

    TE-->>WE: task result
```

### Task Execution: Critical Path (Simplified)

The essential sequence through the critical path components, omitting supporting concerns (Isolation Policy, EE Registry, Resource Monitor, Lifecycle Manager).

```mermaid
sequenceDiagram
    participant WE as Workflow Engine
    participant TE as Task Executor
    participant R as Reconciler
    participant PR as Pool Registry
    participant P as Provisioner
    participant CP as Credential Provider
    participant D as Dispatcher
    participant W as Worker

    WE->>TE: execute_task(task_definition)

    TE->>R: resolve_pool(labels, workload_type)
    R->>PR: query registered pools
    PR-->>R: matching pools with capacity
    R-->>TE: ReconcileResult (MATCHED, pool A)

    TE->>P: acquire(pool_A, image, resources)
    P-->>TE: WorkerHandle (pod_name, namespace)

    TE->>D: dispatch(handle, payload, credential_refs)
    D->>CP: resolve credentials
    CP-->>D: decrypted credentials
    D->>W: exec task (stdin JSON + credentials)
    W-->>D: task output (stdout JSON)
    D-->>TE: TaskResult

    TE->>P: release(handle)
    TE-->>WE: TaskResult
```

### Reconciliation: Pool Selection Logic

Detail of how the Reconciler evaluates candidate pools. The Task Executor calls the Reconciler; the Workflow Engine is not involved.

```mermaid
sequenceDiagram
    participant TE as Task Executor
    participant R as Reconciler
    participant RM as Resource Monitor
    participant IP as Isolation Policy

    TE->>R: resolve pool (labels, workload type, connectivity)

    Note over R: Task node specifies:<br/>- affinity labels: {gpu: "true", region: "eu-west"}<br/>- workload type: agentic<br/>- connectivity: needs access to vault.internal:8200

    R->>IP: get pool eligibility for "agentic" workload
    IP-->>R: must use isolated namespace,<br/>exclude pools without agentic support

    R->>RM: list pools with status
    RM-->>R: Pool A: healthy, 3/10 workers active, labels: {gpu: true, region: eu-west}<br/>Pool B: healthy, 8/10 workers active, labels: {gpu: true, region: us-east}<br/>Pool C: degraded, labels: {region: eu-west}

    Note over R: Step 1 — Label match:<br/>Pool A: labels match ✓<br/>Pool B: region mismatch ✗<br/>Pool C: labels match ✓

    Note over R: Step 2 — Isolation eligibility:<br/>Pool A: agentic supported ✓<br/>Pool C: agentic not supported ✗

    Note over R: Step 3 — Connectivity:<br/>Pool A: can reach vault.internal:8200 ✓

    Note over R: Step 4 — Capacity:<br/>Pool A: 7 slots available ✓

    R-->>TE: ranked list [Pool A]
```

### Provisioning Failure and Fallback

What happens when provisioning fails in the selected pool. The Task Executor handles the retry logic internally — the Workflow Engine is unaware of the fallback.

```mermaid
sequenceDiagram
    participant WE as Workflow Engine
    participant TE as Task Executor
    participant R as Reconciler
    participant P as Provisioner
    participant LM as Lifecycle Manager
    participant RM as Resource Monitor

    WE->>TE: execute_task(task definition)

    TE->>R: resolve pool
    R-->>TE: ranked list [Pool A, Pool B]

    TE->>P: provision worker in Pool A
    P->>LM: create worker
    LM-->>P: failure (image pull error)
    P-->>TE: provisioning failed

    Note over TE: Retry with next pool in ranked list

    TE->>P: provision worker in Pool B
    P->>LM: create worker
    LM-->>P: worker created
    P-->>TE: worker provisioned

    Note over TE: Continue with dispatch...

    Note over RM: Resource Monitor detects<br/>Pool A image pull failures,<br/>marks pool as degraded
```

### Worker Lifecycle

The lifecycle of a Worker from creation through cleanup.

```mermaid
stateDiagram-v2
    [*] --> Pending: Provisioner requests creation
    Pending --> Pulling: Lifecycle Manager pulls EE image
    Pulling --> Starting: Image pulled, container starting
    Starting --> Running: Container ready, task dispatched
    Running --> Succeeded: Task completed successfully
    Running --> Failed: Task error or timeout
    Running --> Terminated: Cancellation signal received
    Pulling --> Failed: Image pull failure
    Starting --> Failed: Container start failure
    Succeeded --> Cleanup: Lifecycle Manager reclaims resources
    Failed --> Cleanup: Lifecycle Manager reclaims resources
    Terminated --> Cleanup: Lifecycle Manager reclaims resources
    Cleanup --> [*]
```

### Credential Injection with Isolation

How credentials are scoped by workload type.

```mermaid
sequenceDiagram
    participant D as Dispatcher
    participant CP as Credential Provider
    participant IP as Isolation Policy
    participant W as Worker

    Note over D: Task node: agentic workload<br/>Requests: llm-api-key, vault-token

    D->>CP: resolve credentials (task config, workload type: agentic)

    CP->>IP: can "agentic" workload access "llm-api-key"?
    IP-->>CP: granted (agentic workloads may use LLM credentials)

    CP->>IP: can "agentic" workload access "vault-token"?
    IP-->>CP: denied (automation credential, restricted from agentic workloads)

    Note over CP: Return only permitted credentials

    CP-->>D: {llm-api-key: "sk-..."}

    D->>W: inject credentials as env vars (ephemeral, not persisted)
```

### Back-Pressure: Capacity Exhaustion and Scale-Up

Shows the flow when all matching Worker Pools are at capacity. The Reconciler detects the condition, the Task Executor decides what to do, and the Provisioner attempts to add capacity.

```mermaid
sequenceDiagram
    participant WE as Workflow Engine
    participant TE as Task Executor
    participant R as Reconciler
    participant RM as Resource Monitor
    participant P as Provisioner
    participant WP as Worker Pool

    WE->>TE: execute_task(task_definition)

    TE->>R: resolve_pool(task_requirements)

    R->>RM: get pool capacity + health
    RM-->>R: pool capacity data

    Note over R: Pools A and B match labels<br/>but both are at max workers

    R-->>TE: ReconcileResult {<br/>  outcome: CAPACITY_EXHAUSTED,<br/>  exhausted_pools: [A, B],<br/>  available_pools: [] }

    Note over TE: No available pools —<br/>attempt corrective action

    TE->>P: request_scale_up(pool_A, +1 worker)

    alt Pool can scale
        P->>WP: scale Deployment replicas +1
        WP-->>P: new pod scheduled
        P-->>TE: scale_up accepted

        Note over TE: Re-poll until new worker<br/>is ready or timeout expires

        loop Poll for capacity (with timeout)
            TE->>R: resolve_pool(task_requirements)
            R->>RM: get pool capacity
            RM-->>R: updated capacity
            R-->>TE: ReconcileResult { outcome: MATCHED, available_pools: [A] }
        end

        Note over TE: Capacity available —<br/>proceed with normal flow

        TE->>P: acquire(pool_A, image, resources)
        P-->>TE: WorkerHandle

    else Pool at configured maximum
        P-->>TE: scale_up declined (at max)

        Note over TE: Cannot scale —<br/>queue and wait for a worker<br/>to free up (with timeout)

        loop Wait for capacity (with timeout)
            TE->>R: resolve_pool(task_requirements)
            R-->>TE: ReconcileResult
        end

        alt Capacity freed before timeout
            TE->>P: acquire(pool, image, resources)
            P-->>TE: WorkerHandle
        else Timeout expired
            TE-->>WE: TaskResult { status: FAILED,<br/>  error: "capacity_timeout" }
        end
    end
```

**Design decisions:**

- **Reconciler detects, Task Executor decides.** The Reconciler is a pure query — it never mutates state. The Task Executor holds the policy for what to do when capacity is exhausted (queue, scale, fail).
- **Scale-up via the Provisioner.** The Provisioner already knows the pool's infrastructure (K8s Deployment, Agent Sandbox CRD, etc.), so it is the natural component to request additional capacity from. No separate Autoscaler component is needed for the MVP.
- **Bounded retries with timeout.** The Task Executor re-polls the Reconciler rather than blocking on a notification. This keeps the design simple and avoids event-bus coupling. The overall task timeout (from the task definition) bounds how long the Task Executor will wait.
- **No dedicated Task Queue component for MVP.** The Task Executor holds the waiting task in-process. A persistent, shared task queue can be introduced later if cross-instance coordination or priority scheduling is needed.

## Entity Relationships

```mermaid
erDiagram
    WORKER_POOL ||--o{ WORKER : contains
    WORKER_POOL ||--o{ POOL_LABEL : has
    WORKER_POOL ||--o{ CONNECTIVITY_ENDPOINT : reaches
    WORKER_POOL }o--|| PROVISIONER_TYPE : "managed by"
    WORKER_POOL ||--|| LIFECYCLE_MANAGER : "operated by"

    WORKER }o--|| EXECUTION_ENVIRONMENT : "runs in"
    WORKER }o--|| ISOLATION_POLICY : "governed by"
    WORKER }o--|| TASK_DISPATCH : "executes"

    TASK_DISPATCH }o--|| CREDENTIAL_SET : "injected with"

    EXECUTION_ENVIRONMENT }o--|| OCI_REGISTRY : "sourced from"

    WORKFLOW_NODE }o--o{ NODE_LABEL : has
    WORKFLOW_NODE ||--|| TASK_DISPATCH : "produces"

    WORKER_POOL {
        uuid id PK
        string name
        string status
        int max_workers
        string provisioner_type
    }

    POOL_LABEL {
        uuid id PK
        uuid pool_id FK
        string key
        string value
    }

    CONNECTIVITY_ENDPOINT {
        uuid id PK
        uuid pool_id FK
        string host
        int port
        string protocol
    }

    WORKER {
        uuid id PK
        uuid pool_id FK
        uuid ee_id FK
        uuid policy_id FK
        string state
        datetime created_at
        datetime terminated_at
    }

    EXECUTION_ENVIRONMENT {
        uuid id PK
        string name
        string image_ref
        string base_image
        string workload_type
    }

    ISOLATION_POLICY {
        uuid id PK
        string name
        string workload_type
        string namespace_mode
        json network_policy
        json security_context
    }

    TASK_DISPATCH {
        uuid id PK
        uuid worker_id FK
        uuid node_id FK
        string status
        json input_data
        json output_data
        datetime dispatched_at
        datetime completed_at
    }

    WORKFLOW_NODE {
        uuid id PK
        string workload_type
        uuid ee_id FK
    }

    NODE_LABEL {
        uuid id PK
        uuid node_id FK
        string key
        string value
    }
```

## MVP Scope (On-Cluster OpenShift)

Per the July 2026 planning decisions ([ANSTRAT-1803](https://redhat.atlassian.net/browse/ANSTRAT-1803) comments):

| Component | MVP Scope |
|---|---|
| Task Executor | Single `execute_task` entry point for the Workflow Engine; orchestrates the full pipeline including back-pressure handling (in-process queue, scale-up requests, bounded timeout) |
| Worker | OpenShift pod within the control plane cluster |
| Worker Pool | Single on-cluster pool (same namespace or dedicated namespace) |
| Reconciler | Global affinity only; project/workflow/node-level affinity deferred. Returns structured `ReconcileResult` with outcome (`MATCHED` / `CAPACITY_EXHAUSTED` / `NO_MATCHING_POOLS`) |
| Provisioner | OpenShift pod provisioner (Kubernetes API). Implementation-agnostic interface. Supports `acquire`/`release` and `request_scale_up` (bounded by pool max) |
| Dispatcher | Deliver task payload, collect results |
| EE Registry | OCI image references from customer-accessible registries |
| Resource Monitor | Pod/agent list view for administrators |
| Credential Provider | Credential injection scoped by workload type |
| Isolation Policy | Container isolation + namespace separation (no OpenShell/policy-based sandboxing) |
| Lifecycle Manager | Pod lifecycle management (create, monitor, cleanup) |

### Future Phases

- **[ANSTRAT-2337](https://redhat.atlassian.net/browse/ANSTRAT-2337):** External OpenShift cluster execution (polling and push connectivity to remote OCP clusters)
- **[ANSTRAT-2338](https://redhat.atlassian.net/browse/ANSTRAT-2338):** External RHEL execution (hop nodes and execution nodes on RHEL)
- Per-project, per-workflow, and per-node affinity rules
- Policy-based sandboxing (OpenShell integration)
- EE build tooling (extending ansible-builder or new tool)
