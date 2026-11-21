"""Steady mixed API traffic generator."""
from __future__ import annotations

import random
import time

import httpx
import typer

app = typer.Typer(help="Generate baseline traffic for the local testbed.")


def run_load(minutes: float = 1.0, rps: float = 5.0, seed: int | None = None) -> None:
    if minutes <= 0 or rps <= 0:
        raise ValueError("minutes and rps must be positive")
    rng = random.Random(seed)
    end = time.monotonic() + minutes * 60
    users = [1, 2, 3]
    with httpx.Client(timeout=3.0) as client:
        while time.monotonic() < end:
            cycle_rate = max(0.1, rps * rng.uniform(0.85, 1.15))
            cycle_start = time.monotonic()
            user_id = rng.choice(users)
            try:
                if rng.random() < 0.55:
                    client.get(f"http://127.0.0.1:8101/users/{user_id}")
                else:
                    client.post(
                        "http://127.0.0.1:8101/orders",
                        json={"user_id": user_id, "amount": round(rng.uniform(3, 300), 2)},
                    )
            except httpx.HTTPError:
                pass
            delay = 1 / cycle_rate - (time.monotonic() - cycle_start)
            if delay > 0:
                time.sleep(delay)


@app.command()
def main(
    minutes: float = typer.Option(1.0, min=0.01),
    rps: float = typer.Option(5.0, min=0.1),
    seed: int | None = None,
) -> None:
    """Send baseline mixed GET/POST requests to the API service."""
    run_load(minutes, rps, seed)


if __name__ == "__main__":
    app()