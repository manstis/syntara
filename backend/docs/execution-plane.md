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
    R -->|"selected pool"| TE
    TE -->|"provision worker"| P
    EER -->|"container image"| P
    IP -->|"isolation constraints"| P
    P -->|"worker created"| TE
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

### Task Executor

The single entry point into the Execution Plane. The Workflow Engine calls `execute_task(task_definition)` and receives a result — it has no knowledge of pools, provisioning, or dispatch internals. The Task Executor owns the full orchestration pipeline: Reconciler, Provisioner, Dispatcher, and cleanup.

**Responsibilities:**
- Accept a task definition from the Workflow Engine (task payload, affinity labels, workload type, connectivity requirements, EE reference)
- Orchestrate the execution pipeline: reconcile a pool, provision a worker, dispatch the task, collect the result
- Handle provisioning failures by retrying with fallback pools from the Reconciler's ranked list
- Return the task result (or a structured error) to the Workflow Engine
- Ensure worker cleanup occurs regardless of task outcome (delegates to the Lifecycle Manager)

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

Selects a Worker Pool in which to provision a Worker for a given task node. The Reconciler evaluates task requirements against available Worker Pools, considering labels, connectivity, capacity, and isolation constraints.

**Responsibilities:**
- Match task node affinity labels against Worker Pool labels
- Evaluate connectivity requirements (can the pool reach the endpoints this task needs?)
- Consult the Resource Monitor for available capacity
- Apply Isolation Policy constraints (e.g. agentic workloads excluded from certain pools)
- Return a ranked list of eligible Worker Pools (or fail if none match)

**Inputs:**
- Task node label selectors and affinity rules
- Task node connectivity requirements
- Workload type (deterministic action vs. agentic)
- Resource Monitor data (capacity, health, connectivity metadata)

### Provisioner

Creates a Worker within the selected Worker Pool. The Provisioner is an abstraction over the underlying infrastructure — different implementations handle different pool types (OpenShift pods, OpenShell sandboxes, Agent Sandbox, etc.).

**Responsibilities:**
- Create the Worker compute resource in the target pool
- Pull and apply the specified Execution Environment container image
- Apply Isolation Policy constraints (namespace isolation, network policies, seccomp profiles)
- Configure resource limits per the Worker definition
- Report provisioning status (success, failure, timeout)

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
| Task Executor | Single `execute_task` entry point for the Workflow Engine; orchestrates the full pipeline |
| Worker | OpenShift pod within the control plane cluster |
| Worker Pool | Single on-cluster pool (same namespace or dedicated namespace) |
| Reconciler | Global affinity only; project/workflow/node-level affinity deferred |
| Provisioner | OpenShift pod provisioner (Kubernetes API). Implementation-agnostic interface |
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
