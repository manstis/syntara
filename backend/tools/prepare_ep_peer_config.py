"""Generate the initial localhost command trust file without replacing operator edits."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def prepare_host_config(template: Path, output: Path) -> None:
    """Use the existing dev signing keys and restrict host routes to loopback."""
    if output.exists():
        return
    trust = json.loads(template.read_text())
    for peer in trust["peers"]:
        base = (
            "https://localhost:8001/v1"
            if peer["role"] == "executor"
            else "https://localhost:8000/api/execution_plane/v1"
        )
        peer["command_url"] = base + "/commands"
        peer["query_base_url"] = base
        peer["permitted_networks"] = ["127.0.0.0/8", "::1/128"]
        if peer["role"] == "controller":
            peer["verification_key_paths"] = {"orchestrator-primary": "jwt-primary.pub"}
    output.write_text(json.dumps(trust, indent=2) + "\n")


def main() -> None:
    """Create the host variant in the caller's secret directory."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    prepare_host_config(args.template, args.output)


if __name__ == "__main__":
    main()
