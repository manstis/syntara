"""Build source preparation accepts only the separate, pinned client project."""

# ruff: noqa: S603, S607 - fixed Git commands in disposable test repositories

import hashlib
import io
import json
import tarfile
import tomllib
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.ci.ep_client_source import prepare_client_source
from tools.ci.sanitize_pyproject_for_hermetic import sanitize_pyproject


@pytest.fixture
def source_case(tmp_path):
    local = tmp_path / "syntara-execution-plane/src/execution_plane_client"
    local.mkdir(parents=True)
    (local / "pyproject.toml").write_text('[project]\nname = "execution-plane-client"\nversion = "0.1.0"\n')
    (local / "__init__.py").write_text('"""Shared client."""\n')
    (local / "py.typed").touch()
    for directory in (".git", ".venv", ".secrets", "execution_plane"):
        (local / directory).mkdir()
        (local / directory / "private.py").write_text("must_not_copy = True\n")
    return SimpleNamespace(local=local, destination=tmp_path / "installed", archive=tmp_path / "source.tar.gz")


def archive_source(case, *, malicious=None):
    with tarfile.open(case.archive, "w:gz") as archive:
        for path in (case.local / "pyproject.toml", case.local / "__init__.py"):
            archive.add(path, arcname="repo/src/execution_plane_client/" + path.name)
        other = tarfile.TarInfo("repo/src/execution_plane/worker.py")
        other.size = 7
        archive.addfile(other, io.BytesIO(b"private"))
        if malicious is not None:
            archive.addfile(malicious, io.BytesIO(b""))
    return hashlib.sha256(case.archive.read_bytes()).hexdigest()


def test_source_preparation_never_installs_ep_service(source_case):
    prepare_client_source(source_case.local, None, source_case.destination, None)
    copied = {path.name for path in source_case.destination.iterdir()}
    assert copied == {"pyproject.toml", "__init__.py", "py.typed"}
    assert (
        tomllib.loads((source_case.destination / "pyproject.toml").read_text())["project"]["name"]
        == "execution-plane-client"
    )


def test_missing_sources_fail_explicitly(source_case):
    with pytest.raises(ValueError, match="source"):
        prepare_client_source(None, None, source_case.destination, None)


@pytest.mark.parametrize("checksum", [None, "0" * 64])
def test_archive_requires_matching_checksum(source_case, checksum):
    archive_source(source_case)
    with pytest.raises(ValueError, match="checksum"):
        prepare_client_source(None, source_case.archive, source_case.destination, checksum)
    assert not source_case.destination.exists()


def test_verified_archive_extracts_only_client(source_case):
    digest = archive_source(source_case)
    prepare_client_source(None, source_case.archive, source_case.destination, digest)
    assert (source_case.destination / "__init__.py").exists()
    assert not (source_case.destination / "execution_plane").exists()


@pytest.mark.parametrize("path", ["../escape", "/escape", "repo/src/execution_plane_client/../../escape"])
def test_archive_rejects_traversal(source_case, path):
    digest = archive_source(source_case, malicious=tarfile.TarInfo(path))
    with pytest.raises(ValueError, match="Unsafe"):
        prepare_client_source(None, source_case.archive, source_case.destination, digest)
    assert not source_case.destination.exists()


@pytest.mark.parametrize("link_type", [tarfile.SYMTYPE, tarfile.LNKTYPE])
def test_archive_rejects_links(source_case, link_type):
    entry = tarfile.TarInfo("repo/src/execution_plane_client/leak.py")
    entry.type, entry.linkname = link_type, "../../../.secrets/private.py"
    digest = archive_source(source_case, malicious=entry)
    with pytest.raises(ValueError, match="Unsafe"):
        prepare_client_source(None, source_case.archive, source_case.destination, digest)


def test_local_source_rejects_links(source_case):
    (source_case.local / "leak.py").symlink_to(source_case.local / ".secrets/private.py")
    with pytest.raises(ValueError, match="Unsafe"):
        prepare_client_source(source_case.local, None, source_case.destination, None)


def test_wrong_distribution_fails_before_copy(source_case):
    (source_case.local / "pyproject.toml").write_text('[project]\nname = "execution-plane"\n')
    with pytest.raises(ValueError, match="execution-plane-client"):
        prepare_client_source(source_case.local, None, source_case.destination, None)


def test_hermetic_sanitizer_preserves_only_client_local_source(tmp_path):
    backend = Path(__file__).resolve().parents[3]
    project = tmp_path / "pyproject.toml"
    project.write_bytes((backend / "pyproject.toml").read_bytes())
    sanitize_pyproject(project)
    sources = tomllib.loads(project.read_text())["tool"]["uv"]["sources"]
    assert sources["execution-plane-client"]["path"] == "../../syntara-execution-plane/src/execution_plane_client"
    assert "execution-plane" not in sources
    assert "external-services" not in sources


def test_requirements_keep_dependencies_but_not_local_client():
    backend = Path(__file__).resolve().parents[3]
    requirements = (backend / "requirements.txt").read_text()
    assert "../../syntara-execution-plane" not in requirements
    assert "cryptography==" in requirements
    assert "httpx==" in requirements
    assert "--no-emit-package execution-plane-client" in (backend / "Makefile").read_text()


def test_source_lock_and_hermeto_artifact_agree():
    import yaml

    backend = Path(__file__).resolve().parents[3]
    lock = json.loads((backend / "ep-client-source.json").read_text())
    artifacts = yaml.safe_load((backend / "artifacts.lock.yaml").read_text())["artifacts"]
    artifact = next(item for item in artifacts if item.get("filename") == "execution-plane-client-source.tar.gz")
    assert artifact["download_url"] == lock["archive_url"]
    assert artifact["checksum"] == "sha256:" + lock["archive_sha256"]
    assert len(lock["revision"]) == 40


def test_pin_uses_committed_client_bytes_and_exact_archive_checksum(tmp_path):
    import subprocess

    import yaml

    from tools.ci.pin_ep_client_source import pin_client_source

    repo = tmp_path / "ep"
    client = repo / "src/execution_plane_client"
    client.mkdir(parents=True)
    (client / "pyproject.toml").write_text('[project]\nname="execution-plane-client"\nversion="0.1.0"\n')
    (client / "__init__.py").write_text('version = "committed"\n')
    subprocess.run(["git", "init", str(repo)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True, capture_output=True)
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.test",
            "commit",
            "-m",
            "fixture",
        ],
        check=True,
        capture_output=True,
    )
    (client / "__init__.py").write_text('version = "uncommitted"\n')
    artifact_lock = tmp_path / "artifacts.lock.yaml"
    artifact_lock.write_text('metadata: {version: "1.0"}\nartifacts: []\n')
    archive = tmp_path / "source.tar.gz"
    lock = pin_client_source(
        repo,
        archive,
        tmp_path / "source.json",
        artifact_lock,
        repository="example/ep",
        archive_url="https://example.test/{revision}/source.tar.gz",
    )
    assert lock["revision"] == subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    assert lock["archive_sha256"] == hashlib.sha256(archive.read_bytes()).hexdigest()
    prepare_client_source(None, archive, tmp_path / "unpacked", lock["archive_sha256"])
    assert 'version = "committed"' in (tmp_path / "unpacked/__init__.py").read_text()
    assert yaml.safe_load(artifact_lock.read_text())["artifacts"][0]["checksum"] == "sha256:" + lock["archive_sha256"]


def test_peer_example_uses_ao_signing_key_id():
    from syntara.core.config.base import get_settings

    backend = Path(__file__).resolve().parents[3]
    trust = json.loads((backend / "containers/execution-plane/command-peers.example.json").read_text())
    ao = next(peer for peer in trust["peers"] if peer["peer_id"] == "syntara-orchestration")
    assert get_settings().jwt_key_id in ao["verification_key_paths"]


def test_host_peer_configuration_uses_local_routes_and_existing_public_keys(tmp_path):
    from tools.prepare_ep_peer_config import prepare_host_config

    backend = Path(__file__).resolve().parents[3]
    output = tmp_path / "peers.json"
    prepare_host_config(backend / "containers/execution-plane/command-peers.example.json", output)
    peers = json.loads(output.read_text())["peers"]
    assert peers[0]["command_url"] == "https://localhost:8001/v1/commands"
    assert peers[1]["command_url"] == "https://localhost:8000/api/execution_plane/v1/commands"
    assert peers[1]["verification_key_paths"] == {"orchestrator-primary": "jwt-primary.pub"}
    assert all(peer["permitted_networks"] == ["127.0.0.0/8", "::1/128"] for peer in peers)
    output.write_text("operator custom registration")
    prepare_host_config(backend / "containers/execution-plane/command-peers.example.json", output)
    assert output.read_text() == "operator custom registration"
