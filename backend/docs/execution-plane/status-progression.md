# Execution Plane status progression

Journal status and domain observation have distinct meanings:

| State | Owner | Meaning |
| --- | --- | --- |
| Journal delivery/acceptance | Shared client | Whether the peer durably accepted the message |
| Journal handling/effect | Shared client with AO handler | Whether local Temporal completion finished or requires observation |
| Command outcome | EP, observed by AO | Whether the requested work/resource action succeeded, failed or remains uncertain |
| `Integration.execution_plane_status` | AO integration | User-visible projection of the current resource revision |

Temporal identity, attempt and token are encrypted journal-private correlation
metadata. They do not form another status lifecycle. See
[journal-handling-status.md](journal-handling-status.md) and
[integration-status.md](integration-status.md).
