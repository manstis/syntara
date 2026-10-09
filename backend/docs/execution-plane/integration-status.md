# Integration.execution_plane_status

AO owns the integration's desired configuration and revision. The integration
service commits its domain change together with an immutable `cluster.provision`
or `cluster.delete` command, including the credential snapshot. Shared journal
workers deliver, acknowledge and recover that command.

The completion handler applies an observation only when its resource key and
revision match the current integration. A succeeded enabled provision becomes
`ready`; draining/deletion remains `deleting`; a failed or superseded action
becomes `error` with a safe code. Deleted integrations are never recreated by
late outcomes. New desired revisions return the projection to `pending` or
`deleting` as appropriate.

This field is a UI/domain projection, not a delivery queue. The journal owns retry
and recovery responsibility; there is no integration-sync outbox or polling loop
that rebuilds payloads from mutable integration/credential state.
