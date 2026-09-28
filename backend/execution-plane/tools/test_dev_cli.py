"""Tests for the cross-platform Execution Plane development CLI."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Self

import dev_cli
import pytest

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

from dev_cli import (
    CommandResult,
    EnvironmentDetails,
    EnvironmentProvider,
    EnvironmentSelectionError,
    LocalEnvironment,
    OpenShiftEnvironment,
    _CliDiscoveryMechanism,
    _collect_local_details,
    _collect_openshift_details,
    _find_registered_cluster,
    _refresh_registered_cluster,
    _register_environment_record,
    _remove_environment_record,
    _remove_target_work_items,
    _require_active_registration,
    available_local_providers,
    main,
    select_provider,
)
from execution_plane.cluster.cluster_registry import ClusterRegistration
from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.models.cluster import Cluster, ClusterStatus

_DATABASE_UNAVAILABLE = "database unavailable"


@pytest.fixture(autouse=True)
def suppress_database_registration(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep provider-selection tests independent of the development database."""
    monkeypatch.setattr(dev_cli, "_register_environment", lambda **_: None, raising=False)
    monkeypatch.setattr(dev_cli, "_remove_environment", lambda **_: None, raising=False)


class FakeRunner:
    def __init__(self, *results: CommandResult) -> None:
        self.results = list(results)
        self.commands: list[tuple[str, ...]] = []

    def run(self, args: Sequence[str]) -> CommandResult:
        self.commands.append(tuple(args))
        return self.results.pop(0) if self.results else CommandResult(0)


class _Session:
    def __init__(
        self,
        *,
        result: object | None = None,
        execute_error: Exception | None = None,
        commit_error: Exception | None = None,
    ) -> None:
        self.result = result
        self.execute_error = execute_error
        self.commit_error = commit_error
        self.executed: list[object] = []
        self.commits = 0
        self.rollbacks = 0

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def get(self, _model: object, _item_id: uuid.UUID, **_: object) -> object | None:
        return self.result

    async def execute(self, statement: object) -> None:
        if self.execute_error is not None:
            raise self.execute_error
        self.executed.append(statement)

    async def commit(self) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1


class _SessionStore:
    def __init__(self, session: _Session) -> None:
        self.session = session

    def _session_context(self) -> _Session:
        return self.session


class _StoreContext:
    def __init__(self, store: object) -> None:
        self.store = store

    async def __aenter__(self) -> object:
        return self.store

    async def __aexit__(self, *_: object) -> None:
        return None


def _store_factory(store: object) -> type[object]:
    class StoreFactory:
        @classmethod
        def from_database_url(cls, _database_url: str) -> _StoreContext:
            return _StoreContext(store)

    return StoreFactory


class _ClusterRegistry:
    def __init__(self, clusters: list[object], registered: object | None = None) -> None:
        self.clusters = clusters
        self.registered = registered
        self.register_calls: list[tuple[object, ...]] = []

    async def list(self) -> list[object]:
        return self.clusters

    async def register(self, *args: object) -> object:
        self.register_calls.append(args)
        return self.registered


class _TargetRegistry:
    def __init__(self, targets: list[object]) -> None:
        self.targets = targets
        self.update_calls: list[dict[str, object]] = []

    async def list(self, **_: object) -> list[object]:
        return self.targets

    async def update(self, target_id: uuid.UUID, **kwargs: object) -> None:
        self.update_calls.append({"target_id": target_id, **kwargs})


class _LifecycleStore:
    def __init__(self, items: list[object], calls: list[str], session: _Session | None = None) -> None:
        self.items = items
        self.calls = calls
        self.session = session

    async def list(self, **_: object) -> list[object]:
        return self.items

    async def request_delete(self, *_: object) -> None:
        self.calls.append("request_cluster_delete")

    async def finalize_delete(self, *_: object) -> None:
        self.calls.append("finalize_delete")

    async def get(self, *_: object) -> None:
        return None

    def _session_context(self) -> _Session:
        assert self.session is not None
        return self.session


def executable_checker(*installed: str) -> Callable[[str], str | None]:
    """Return a typed executable lookup for CLI tests."""
    installed_set = set(installed)
    return lambda name: name if name in installed_set else None


def test_auto_selects_the_only_available_local_provider() -> None:
    assert select_provider("auto", [EnvironmentProvider.KIND]) is LocalEnvironment


def test_auto_rejects_ambiguous_local_providers() -> None:
    with pytest.raises(EnvironmentSelectionError, match="both kind and minikube"):
        select_provider("auto", [EnvironmentProvider.KIND, EnvironmentProvider.MINIKUBE])


def test_auto_explains_how_to_install_a_local_provider_when_none_is_available() -> None:
    with pytest.raises(EnvironmentSelectionError, match="install kind or minikube"):
        select_provider("auto", [])


def test_explicit_openshift_selects_remote_environment() -> None:
    assert select_provider("openshift", []) is OpenShiftEnvironment


def test_remote_openshift_does_not_support_local_lifecycle_operations() -> None:
    with pytest.raises(EnvironmentSelectionError, match="does not support 'up'"):
        OpenShiftEnvironment.ensure_lifecycle_operation_allowed("up")


def test_detects_installed_local_providers_without_shell_commands() -> None:
    assert available_local_providers(executable_checker("kind", "oc")) == [EnvironmentProvider.KIND]


def test_kind_up_creates_the_requested_cluster() -> None:
    runner = FakeRunner()

    assert main(["--provider", "kind", "up"], runner=runner, executable_exists=executable_checker("kind")) == 0

    assert runner.commands == [("kind", "create", "cluster", "--name", "execution-plane")]


def test_mixed_case_provider_environment_value_is_normalized(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EP_DEV_PROVIDER", "KIND")
    runner = FakeRunner()

    assert main(["up"], runner=runner, executable_exists=executable_checker("kind")) == 0

    assert runner.commands == [("kind", "create", "cluster", "--name", "execution-plane")]


def test_minikube_down_stops_and_deletes_the_requested_profile() -> None:
    runner = FakeRunner()

    assert (
        main(["--provider", "minikube", "down"], runner=runner, executable_exists=executable_checker("minikube")) == 0
    )

    assert runner.commands == [
        ("minikube", "stop", "--profile", "execution-plane"),
        ("minikube", "delete", "--profile", "execution-plane"),
    ]


def test_local_down_removes_the_registered_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    removed: list[dict[str, object]] = []
    monkeypatch.setattr(dev_cli, "_remove_environment", lambda **kwargs: removed.append(kwargs), raising=False)
    runner = FakeRunner()

    assert (
        main(["--provider", "minikube", "down"], runner=runner, executable_exists=executable_checker("minikube")) == 0
    )

    assert removed == [
        {
            "provider": EnvironmentProvider.MINIKUBE,
            "cluster": "execution-plane",
        }
    ]


def test_kind_status_lists_clusters() -> None:
    runner = FakeRunner(CommandResult(0, "execution-plane\n"))

    assert main(["--provider", "kind", "status"], runner=runner, executable_exists=executable_checker("kind")) == 0

    assert runner.commands == [("kind", "get", "clusters")]


def test_kind_status_fails_when_the_requested_cluster_is_missing() -> None:
    runner = FakeRunner(CommandResult(0, "other-cluster\n"))

    assert main(["--provider", "kind", "status"], runner=runner, executable_exists=executable_checker("kind")) == 1


def test_local_doctor_checks_the_selected_environment() -> None:
    runner = FakeRunner(CommandResult(0, "execution-plane\n"))

    assert main(["--provider", "kind", "doctor"], runner=runner, executable_exists=executable_checker("kind")) == 0

    assert runner.commands == [("kind", "get", "clusters")]


def test_local_connect_is_rejected() -> None:
    runner = FakeRunner()

    assert main(["--provider", "kind", "connect"], runner=runner, executable_exists=executable_checker("kind")) == 1

    assert runner.commands == []


def test_minikube_reset_recreates_the_requested_profile() -> None:
    runner = FakeRunner()

    assert (
        main(
            ["--provider", "minikube", "reset", "--yes"],
            runner=runner,
            executable_exists=executable_checker("minikube"),
        )
        == 0
    )

    assert runner.commands == [
        ("minikube", "delete", "--profile", "execution-plane"),
        ("minikube", "start", "--profile", "execution-plane"),
    ]


def test_openshift_connect_validates_the_selected_context_and_namespace() -> None:
    runner = FakeRunner(CommandResult(0, "developer\n"), CommandResult(0, "execution-plane\n"))

    assert (
        main(
            [
                "--provider",
                "openshift",
                "--context",
                "remote-dev",
                "--namespace",
                "execution-plane",
                "connect",
            ],
            runner=runner,
            executable_exists=executable_checker("oc"),
        )
        == 0
    )

    assert runner.commands == [
        ("oc", "whoami", "--context", "remote-dev"),
        ("oc", "get", "project", "execution-plane", "--context", "remote-dev"),
    ]


def test_openshift_connect_fails_when_namespace_is_not_accessible() -> None:
    runner = FakeRunner(CommandResult(0, "developer\n"), CommandResult(1, stderr="forbidden"))

    assert (
        main(
            ["--provider", "openshift", "--context", "remote-dev", "--namespace", "execution-plane", "connect"],
            runner=runner,
            executable_exists=executable_checker("oc"),
        )
        == 1
    )


def test_openshift_requires_an_explicit_namespace() -> None:
    assert (
        main(
            ["--provider", "openshift", "connect"],
            runner=FakeRunner(),
            executable_exists=executable_checker("oc"),
        )
        == 1
    )


def test_openshift_connect_registers_the_connected_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    registered: list[dict[str, object]] = []
    monkeypatch.setattr(dev_cli, "_register_environment", lambda **kwargs: registered.append(kwargs))
    runner = FakeRunner(CommandResult(0, "developer\n"), CommandResult(0, "execution-plane\n"))

    assert (
        main(
            [
                "--provider",
                "openshift",
                "--context",
                "remote-dev",
                "--namespace",
                "execution-plane",
                "connect",
            ],
            runner=runner,
            executable_exists=executable_checker("oc"),
        )
        == 0
    )

    assert registered == [
        {
            "provider": EnvironmentProvider.OPENSHIFT,
            "cluster": "execution-plane",
            "namespace": "execution-plane",
            "context": "remote-dev",
            "runner": runner,
        }
    ]


def test_kind_up_registers_the_connected_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    registered: list[dict[str, object]] = []
    monkeypatch.setattr(dev_cli, "_register_environment", lambda **kwargs: registered.append(kwargs))
    runner = FakeRunner()

    assert main(["--provider", "kind", "up"], runner=runner, executable_exists=executable_checker("kind")) == 0

    assert registered == [
        {
            "provider": EnvironmentProvider.KIND,
            "cluster": "execution-plane",
            "namespace": "execution-plane",
            "context": None,
            "runner": runner,
        }
    ]


def test_openshift_details_include_server_and_token() -> None:
    runner = FakeRunner(CommandResult(0, "https://api.example.com:6443\n"), CommandResult(0, "token\n"))

    details = _collect_openshift_details(runner, "remote-openshift", "remote-dev", "execution-plane")

    assert details.endpoint == "https://api.example.com:6443"
    assert details.api_key == "token"
    assert details.name == "remote-openshift"
    assert details.namespace == "execution-plane"
    assert runner.commands == [
        ("oc", "whoami", "--show-server", "--context", "remote-dev"),
        ("oc", "whoami", "--show-token", "--context", "remote-dev"),
    ]


def test_local_details_create_namespace_when_missing() -> None:
    runner = FakeRunner(
        CommandResult(1, stderr="not found"),
        CommandResult(0),
        CommandResult(0, "https://127.0.0.1:6443\n"),
        CommandResult(0, "apiVersion: v1\nusers:\n- name: kind\n"),
    )

    details = _collect_local_details(runner, EnvironmentProvider.KIND, "execution-plane", "execution-plane", None)

    assert details.endpoint == "https://127.0.0.1:6443"
    assert details.api_key.startswith("apiVersion: v1")
    assert runner.commands == [
        ("kubectl", "get", "namespace", "execution-plane"),
        ("kubectl", "create", "namespace", "execution-plane"),
        ("kubectl", "config", "view", "--minify", "--raw", "-o", "jsonpath={.clusters[0].cluster.server}"),
        ("kubectl", "config", "view", "--minify", "--raw"),
    ]


def test_cli_discovery_preserves_the_selected_namespace() -> None:
    details = EnvironmentDetails(
        EnvironmentProvider.OPENSHIFT,
        "remote-openshift",
        "https://api.example.com:6443",
        "production",
        "token",
        {"provider": "openshift"},
    )

    result = _CliDiscoveryMechanism(details).discover(
        ClusterRegistration("remote-openshift", details.endpoint, details.api_key, details.labels)
    )

    assert result.targets[0].namespace == "production"
    assert result.targets[0].is_default is True


def test_registered_cluster_lookup_prefers_cli_labels_when_endpoint_changes() -> None:
    details = EnvironmentDetails(
        EnvironmentProvider.MINIKUBE,
        "execution-plane",
        "https://new-endpoint",
        "execution-plane",
        "credentials",
        {"provider": "minikube", "cluster": "execution-plane"},
    )
    labeled_cluster = type(
        "ClusterRecord",
        (),
        {
            "name": "execution-plane",
            "endpoint": "https://old-endpoint",
            "labels": {"provider": "minikube", "cluster": "execution-plane"},
        },
    )()

    assert _find_registered_cluster([labeled_cluster], details) is labeled_cluster


def test_registration_rejects_error_cluster() -> None:
    cluster = type("ClusterRecord", (), {"name": "execution-plane", "status": ClusterStatus.ERROR})()

    with pytest.raises(EnvironmentSelectionError, match="registration failed"):
        _require_active_registration(cluster)


def test_cluster_store_does_not_expose_cli_connection_updates() -> None:
    assert not hasattr(ClusterStore, "update_connection")


@pytest.mark.asyncio
async def test_cli_cleanup_deletes_work_items_for_a_target() -> None:
    session = _Session()

    target_id = uuid.uuid4()
    await _remove_target_work_items(_SessionStore(session), target_id)  # type: ignore[arg-type]

    assert len(session.executed) == 1
    assert session.commits == 1


@pytest.mark.asyncio
async def test_cli_cleanup_rolls_back_when_work_item_deletion_fails() -> None:
    session = _Session(execute_error=RuntimeError(_DATABASE_UNAVAILABLE))

    with pytest.raises(RuntimeError, match=_DATABASE_UNAVAILABLE):
        await _remove_target_work_items(_SessionStore(session), uuid.uuid4())  # type: ignore[arg-type]

    assert session.rollbacks == 1


@pytest.mark.asyncio
async def test_refresh_registered_cluster_updates_connection_details() -> None:
    now = datetime.now(UTC)
    cluster = Cluster(
        id=uuid.uuid4(),
        name="execution-plane",
        endpoint="https://old.example",
        api_key="old-key",
        labels={},
        created_by=dev_cli.CLI_ACTOR_ID,
        created_at=now,
        updated_by=dev_cli.CLI_ACTOR_ID,
        updated_at=now,
    )

    details = EnvironmentDetails(
        EnvironmentProvider.KIND,
        "execution-plane",
        "https://new.example",
        "execution",
        "new-key",
        {"provider": "kind", "cluster": "execution-plane"},
    )

    await _refresh_registered_cluster(_SessionStore(_Session(result=cluster)), cluster, details)  # type: ignore[arg-type]

    assert cluster.endpoint == details.endpoint
    assert cluster.api_key == details.api_key
    assert cluster.labels == details.labels
    assert cluster.updated_by == dev_cli.CLI_ACTOR_ID


@pytest.mark.asyncio
async def test_refresh_registered_cluster_rolls_back_when_cluster_is_missing() -> None:
    session = _Session()

    cluster = type("ClusterRecord", (), {"id": uuid.uuid4(), "name": "execution-plane"})()
    details = EnvironmentDetails(
        EnvironmentProvider.KIND,
        "execution-plane",
        "https://new.example",
        "execution",
        "new-key",
        {},
    )

    with pytest.raises(EnvironmentSelectionError, match="no longer exists"):
        await _refresh_registered_cluster(_SessionStore(session), cluster, details)  # type: ignore[arg-type]

    assert session.rollbacks == 1


@pytest.mark.asyncio
async def test_register_environment_record_registers_a_new_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    cluster = type("ClusterRecord", (), {"status": ClusterStatus.ACTIVE})()
    registry = _ClusterRegistry([], cluster)
    monkeypatch.setattr(dev_cli, "ClusterStore", _store_factory(object()))
    monkeypatch.setattr(dev_cli, "ExecutionTargetStore", _store_factory(object()))
    monkeypatch.setattr(dev_cli, "ClusterRegistry", lambda *_: registry)

    details = EnvironmentDetails(
        EnvironmentProvider.KIND,
        "execution-plane",
        "https://kind.example",
        "execution-plane",
        "key",
        {"provider": "kind", "cluster": "execution-plane"},
    )

    await _register_environment_record(details, "database")

    assert registry.register_calls == [
        (details.name, details.endpoint, details.api_key, dev_cli.CLI_ACTOR_ID, details.labels)
    ]


@pytest.mark.asyncio
async def test_register_environment_record_refreshes_existing_default_target(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    cluster = Cluster(
        id=uuid.uuid4(),
        name="execution-plane",
        endpoint="https://old.example",
        api_key="old-key",
        labels={"provider": "kind", "cluster": "execution-plane"},
        status=ClusterStatus.ACTIVE,
        created_by=dev_cli.CLI_ACTOR_ID,
        created_at=now,
        updated_by=dev_cli.CLI_ACTOR_ID,
        updated_at=now,
    )
    target = type("TargetRecord", (), {"id": uuid.uuid4(), "is_default": True, "namespace": "execution-plane"})()
    target_registry = _TargetRegistry([target])
    monkeypatch.setattr(
        dev_cli,
        "ClusterStore",
        _store_factory(_SessionStore(_Session(result=cluster))),
    )
    monkeypatch.setattr(dev_cli, "ExecutionTargetStore", _store_factory(object()))
    monkeypatch.setattr(dev_cli, "ExecutionTargetRegistry", lambda _: target_registry)
    monkeypatch.setattr(dev_cli, "ClusterRegistry", lambda *_: _ClusterRegistry([cluster]))

    details = EnvironmentDetails(
        EnvironmentProvider.KIND,
        "execution-plane",
        "https://new.example",
        "execution-plane",
        "new-key",
        {"provider": "kind", "cluster": "execution-plane"},
    )

    await _register_environment_record(details, "database")

    assert target_registry.update_calls == [
        {
            "target_id": target.id,
            "updated_by": dev_cli.CLI_ACTOR_ID,
            "endpoint": details.endpoint,
            "api_key": details.api_key,
        }
    ]


@pytest.mark.asyncio
async def test_remove_environment_record_deletes_targets_and_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    cluster = type(
        "ClusterRecord",
        (),
        {
            "id": uuid.uuid4(),
            "name": "execution-plane",
            "labels": {"provider": "kind", "cluster": "execution-plane"},
        },
    )()
    target = type("TargetRecord", (), {"id": uuid.uuid4()})()
    calls: list[str] = []
    cluster_store = _LifecycleStore([cluster], calls)
    target_store = _LifecycleStore([target], calls)
    work_store = _LifecycleStore([], calls, _Session())
    monkeypatch.setattr(dev_cli, "ClusterStore", _store_factory(cluster_store))
    monkeypatch.setattr(dev_cli, "ExecutionTargetStore", _store_factory(target_store))
    monkeypatch.setattr(dev_cli, "WorkStore", _store_factory(work_store))

    await _remove_environment_record(EnvironmentProvider.KIND, "execution-plane", "database")

    assert calls == ["request_cluster_delete", "finalize_delete", "finalize_delete"]
