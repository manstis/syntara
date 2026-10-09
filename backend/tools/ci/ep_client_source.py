#!/usr/bin/env python3
"""Materialize only the shared client for paired local and offline image builds."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import tarfile
import tempfile
import tomllib
from pathlib import Path, PurePosixPath

PACKAGE_SUBDIRECTORY = "src/execution_plane_client"
_EXCLUDED = {".git", ".venv", ".secrets", "__pycache__", "execution_plane", "syntara"}
_MAX_SOURCE_BYTES = 50 * 1024 * 1024
_MAX_SOURCE_FILES = 10_000
_SHA256_HEX_LENGTH = 64


def _check_project(path: Path) -> None:
    if (path / "pyproject.toml").is_symlink():
        msg = "Unsafe metadata symlink in client source"
        raise ValueError(msg)
    try:
        metadata = tomllib.loads((path / "pyproject.toml").read_text())
    except (OSError, ValueError) as exc:
        msg = "Missing or invalid execution-plane-client source metadata"
        raise ValueError(msg) from exc
    if metadata.get("project", {}).get("name") != "execution-plane-client":
        msg = "Source distribution must be execution-plane-client"
        raise ValueError(msg)


def _safe_name(name: str) -> PurePosixPath:
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts or "\\" in name:
        msg = "Unsafe archive source path"
        raise ValueError(msg)
    return path


def _copy_local(source: Path, staging: Path) -> None:
    _check_project(source)
    files = 0
    size = 0
    for root, directories, names in source.walk(follow_symlinks=False):
        directories[:] = [name for name in directories if name not in _EXCLUDED and not name.startswith(".")]
        for name in (*directories, *names):
            if name in _EXCLUDED or name.startswith("."):
                continue
            path = root / name
            if path.is_symlink():
                msg = "Unsafe symlink in client source"
                raise ValueError(msg)
            if path.is_dir():
                continue
            if not path.is_file():
                msg = "Unsafe special file in client source"
                raise ValueError(msg)
            files += 1
            size += path.stat().st_size
            if files > _MAX_SOURCE_FILES or size > _MAX_SOURCE_BYTES:
                msg = "Client source exceeds build limits"
                raise ValueError(msg)
            target = staging / path.relative_to(source)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)


def _archive_root(members: list[tarfile.TarInfo], subpath: PurePosixPath) -> PurePosixPath:
    """Validate all names and link types before selecting the one client subtree."""
    roots = []
    for member in members:
        path = _safe_name(member.name)
        if not (member.isfile() or member.isdir()):
            msg = "Unsafe link or special file in source archive"
            raise ValueError(msg)
        if path.parts[-len(subpath.parts) - 1 :] == (*subpath.parts, "pyproject.toml"):
            roots.append(path.parent)
    if len(roots) != 1:
        msg = "Archive must contain one locked client source directory"
        raise ValueError(msg)
    return roots[0]


def _extract_archive(archive: Path, staging: Path, expected_sha256: str | None, subdirectory: str) -> None:
    if not expected_sha256 or len(expected_sha256) != _SHA256_HEX_LENGTH:
        msg = "A source archive checksum is required"
        raise ValueError(msg)
    with archive.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    if digest != expected_sha256:
        msg = "Source archive checksum mismatch"
        raise ValueError(msg)
    subpath = _safe_name(subdirectory)
    with tarfile.open(archive, "r:*") as bundle:
        members = bundle.getmembers()
        if len(members) > _MAX_SOURCE_FILES or sum(member.size for member in members) > _MAX_SOURCE_BYTES:
            msg = "Source archive exceeds build limits"
            raise ValueError(msg)
        root = _archive_root(members, subpath)
        seen = set()
        for member in members:
            path = PurePosixPath(member.name)
            if not path.is_relative_to(root) or member.isdir():
                continue
            relative = path.relative_to(root)
            if any(part in _EXCLUDED or part.startswith(".") for part in relative.parts):
                continue
            if relative in seen:
                msg = "Unsafe duplicate archive source path"
                raise ValueError(msg)
            seen.add(relative)
            target = staging / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            member_stream = bundle.extractfile(member)
            if member_stream is None:
                msg = "Missing client archive member"
                raise ValueError(msg)
            with member_stream, target.open("wb") as output:
                shutil.copyfileobj(member_stream, output)
    _check_project(staging)


def prepare_client_source(
    local: Path | None,
    archive: Path | None,
    destination: Path,
    expected_sha256: str | None,
    *,
    package_subdirectory: str = PACKAGE_SUBDIRECTORY,
) -> None:
    """Prefer a local project; otherwise require a checksum-verified source archive."""
    if destination.exists() or destination.is_symlink():
        msg = "Refusing to overwrite an existing source destination"
        raise ValueError(msg)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".ep-client-", dir=destination.parent) as directory:
        staging = Path(directory) / "project"
        staging.mkdir()
        if local is not None and (local / "pyproject.toml").is_file():
            _copy_local(local, staging)
        elif archive is not None and archive.is_file():
            _extract_archive(archive, staging, expected_sha256, package_subdirectory)
        else:
            msg = "No client source: supply the sibling project or a verified prefetched archive"
            raise ValueError(msg)
        staging.rename(destination)


def main() -> None:
    """Select local or locked prefetched source without network access."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", type=Path)
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--source-lock", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    args = parser.parse_args()
    lock = json.loads(args.source_lock.read_text())
    prepare_client_source(
        args.local,
        args.archive,
        args.destination,
        lock.get("archive_sha256"),
        package_subdirectory=lock["package_subdirectory"],
    )


if __name__ == "__main__":
    main()
