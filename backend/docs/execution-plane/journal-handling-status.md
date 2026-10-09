# Journal-owned Temporal handling

The original outbound `work.execute` journal record stores encrypted private
context containing Temporal activity identity, attempt number and task token.
A Temporal retry updates that context generation and re-arms the latest received
outcome. The immutable work command remains unchanged.

The shared journal handling lifecycle records `pending`, `processing`, `prepared`,
`deferred`, `reconciling` and `handled`. Preparing an effect freezes its private
input and context generation before any Temporal RPC. Success marks the effect
handled. An ambiguous RPC is reconciled against Temporal history without replay.

A stale context generation cannot finish a refreshed attempt. A newer completion
waits for an uncertain predecessor; an already successful effect prevents another
RPC for the same token generation. Workflow cancellation finds commands by a
private correlation key and uses a deterministic cancellation command identity.

There is no separate activity-binding table, status lifecycle, completion inbox
or delivery retry queue. The context never appears in peer envelopes or public
queries. See [integration.md](integration.md) for the protocol.
