# Integration.execution_plane_status

`Integration.execution_plane_status` is AO's user-visible state machine for
the observed Execution Plane representation of an OpenShift integration. AO
owns the desired configuration and revision; EP owns the observed resource
state. The status is updated by the integration service when it queues a
revision, by the sync outbox when delivery fails, and by polling EP's status
resource.

## Sequence

```mermaid
sequenceDiagram
    autonumber
    participant U as Integration service
    participant AO as AO database
    participant O as Sync outbox
    participant EP as Execution Plane
    participant P as Sync bridge

    U->>AO: create/update OpenShift integration
    U->>AO: increment execution_plane_revision
    U->>AO: status = pending
    U->>O: enqueue upsert with source revision

    loop sync bridge
        P->>O: claim due outbox row
        P->>AO: resolve current desired state and credential
        P->>EP: upsert cluster binding(revision, desired state)
        alt delivery succeeds
            EP-->>P: accepted
            P->>O: mark row processed
        else delivery fails
            P->>O: reschedule row with backoff
            P->>AO: status = error if revision is still current
        end
    end

    loop observed status reconciliation
        P->>AO: select pending/error integrations
        P->>EP: get cluster binding
        alt observed revision is current
            EP-->>P: ready / error / pending / reconciling / deleting / deleted
            P->>AO: map observed state to execution_plane_status
        else observed revision is stale
            EP-->>P: older observed_revision
            Note over P,AO: Ignore stale state
        end
    end

    opt integration is deleted
        U->>AO: status = deleting
        U->>O: enqueue delete with source revision
        P->>EP: delete cluster binding
        EP-->>P: accepted
        Note over AO,EP: AO retains deleting for the delete operation;
        Note over AO,EP: this polling loop selects only pending/error rows.
    end
```

## State machine

```mermaid
stateDiagram-v2
    [*] --> pending: create/update queues upsert

    pending --> ready: EP reports ready at current revision
    pending --> pending: EP reports pending or reconciling
    pending --> error: outbox delivery fails or EP reports error
    pending --> deleting: delete queues delete

    ready --> pending: new upsert revision
    ready --> deleting: delete queues delete

    error --> pending: new upsert revision
    error --> ready: EP later reports ready at current revision
    error --> error: delivery retry fails or EP reports error
    error --> deleting: delete queues delete

    deleting --> pending: a newer upsert revision is queued

    note right of pending
        Polling only reconciles pending/error rows.
        A stale observed_revision is ignored.
    end note
```

`deleted` is deliberately mapped to `deleting`; the AO enum represents the
delete operation in progress rather than adding a separate terminal value.
