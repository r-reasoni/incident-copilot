"""
cli/main.py
===========
Typer-based command-line interface for Incident Copilot.

No commands are implemented yet – this stub ensures the entry-point
registered in pyproject.toml (``incident-copilot``) works without
errors from day one.

Commands will be added in later prompts:
  ingest, tail, list, show, resolve, dismiss, replay, eval
"""
from __future__ import annotations

import typer

app = typer.Typer(
    name="incident-copilot",
    help="Cheap log anomaly detection with LLM root-cause analysis.",
    add_completion=False,
)


@app.command()
def version() -> None:
    """Print the current version."""
    typer.echo("incident-copilot 0.1.0")


if __name__ == "__main__":
    app()
