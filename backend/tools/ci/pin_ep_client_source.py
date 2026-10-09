#!/usr/bin/env python3
"""Create an exact client source asset and synchronize AO's CI/Hermeto pins.

The resulting asset is ready for the repository's normal publication process.
This command neither pushes the revision nor publishes the asset.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import yaml

from tools.ci.ep_client_source import PACKAGE_SUBDIRECTORY, prepare_client_source


def pin_client_source(
    checkout: Path,
    archive: Path,
    source_lock: Path,
    artifact_lock: Path,
    *,
    repository: str,
    archive_url: str,
) -> dict[str, str]:
    """Pin actual committed source and the exact generated archive bytes."""
    if re.fullmatch(r"[\w.-]+/[\w.-]+", repository) is None:
        msg = "Expected a GitHub owner/repository"
        raise ValueError(msg)
    git = shutil.which("git")
    if git is None:
        msg = "Git is required to pin committed source"
        raise ValueError(msg)
    revision = subprocess.check_output([git, "-C", str(checkout), "rev-parse", "HEAD^{commit}"], text=True).strip()  # noqa: S603 - fixed Git operation, explicit checkout argument
    url = archive_url.replace("{revision}", revision)
    if not url.startswith("https://") or "{" in url:
        msg = "An explicit HTTPS publication URL is required"
        raise ValueError(msg)
    raw = subprocess.check_output(  # noqa: S603 - verified commit and fixed package path
        [
            git,
            "-C",
            str(checkout),
            "archive",
            "--format=tar",
            "--prefix=source/",
            revision,
            PACKAGE_SUBDIRECTORY,
        ]
    )
    archive.parent.mkdir(parents=True, exist_ok=True)
    archive.write_bytes(gzip.compress(raw, mtime=0))
    checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
    with tempfile.TemporaryDirectory() as directory:
        prepare_client_source(None, archive, Path(directory) / "client", checksum)
    lock = {
        "repository": repository,
        "revision": revision,
        "archive_url": url,
        "archive_sha256": checksum,
        "package_subdirectory": PACKAGE_SUBDIRECTORY,
    }
    artifacts = yaml.safe_load(artifact_lock.read_text())
    filename = "execution-plane-client-source.tar.gz"
    artifacts["artifacts"] = [item for item in artifacts["artifacts"] if item.get("filename") != filename]
    artifacts["artifacts"].append({"download_url": url, "checksum": "sha256:" + checksum, "filename": filename})
    source_lock.write_text(json.dumps(lock, indent=2) + "\n")
    artifact_lock.write_text(yaml.safe_dump(artifacts, sort_keys=False, explicit_start=True))
    return lock


def main() -> None:
    """Generate an asset and pin for the current committed EP implementation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout", required=True, type=Path)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--repository", required=True)
    parser.add_argument(
        "--archive-url", required=True, help="Publication URL; {revision} expands to the actual Git SHA"
    )
    parser.add_argument("--source-lock", type=Path, default=Path("ep-client-source.json"))
    parser.add_argument("--artifact-lock", type=Path, default=Path("artifacts.lock.yaml"))
    args = parser.parse_args()
    pin_client_source(
        args.checkout,
        args.archive,
        args.source_lock,
        args.artifact_lock,
        repository=args.repository,
        archive_url=args.archive_url,
    )
    print("Source pin written; publish the exact asset and revision through the normal repository workflow.")


if __name__ == "__main__":
    main()
