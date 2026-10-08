# Execution Plane status progression overview

The activity binding and integration status fields are separate state machines
with different owners and lifecycles:

| Field | Scope | Initial transition | Terminal or steady states |
| --- | --- | --- | --- |
| `ExecutionPlaneActivityBinding.status` | One Temporal activity request | `submitting` during durable dispatch preparation | `completed_synchronously`, `completed`, or `reconciliation_required` |
| `Integration.execution_plane_status` | One OpenShift integration revision | `pending` for upsert, `deleting` for delete | `ready`, `error`, or continued `deleting` |

## Independent progression

```mermaid
sequenceDiagram
    autonumber
    participant W as Workflow / API caller
    participant AO as AO persistence
    participant EP as Execution Plane

    par Activity execution
        W->>AO: create binding
        AO->>AO: submitting
        W->>EP: submit work
        EP-->>W: accepted or terminal response
        AO->>AO: handoff_pending / completed_synchronously
        EP-->>AO: completion callback or status result
        AO->>AO: delivering -> completed
    and Integration configuration
        W->>AO: create/update integration
        AO->>AO: pending
        AO->>EP: deliver desired integration state
        EP-->>AO: observed revision and status
        AO->>AO: ready / error / deleting
    end

    Note over AO,EP: Neither field is used as a proxy for the other.
```

The activity binding answers “what is the delivery outcome for this Temporal
activity request?” The integration status answers “what state has EP observed
for this integration revision?”
