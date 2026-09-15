# Execution Plane: Kubernetes Backend Integration

> **Stub** — this document is a placeholder. Additional backend types (OpenShell, etc.) will follow the same structure in sibling documents.

## Scope

This document will cover the `vanilla_k8s` backend type: how the EP worker schedules, monitors, and reclaims pods on a Kubernetes cluster to execute scripts.

## Topics (to be filled in)

### Pod lifecycle

- Pod template construction from `ExecutionTarget.labels` and `WorkItem.payload`
- Warm pool: pre-warming pods before work arrives, reclaiming idle pods
- Pod claim: how a `WorkItem` is bound to a running pod

### Worker group routing

- How `WorkItem` label selectors are matched to `ExecutionTarget` groups
- Priority and affinity rules

### Failure modes

- Pod eviction during execution
- Node failure / unreachable kubelet
- Timeout handling and work item retry policy

### Configuration

- Required fields on `ExecutionTarget` for `backend_type = vanilla_k8s`
- RBAC / ServiceAccount requirements in the target cluster
