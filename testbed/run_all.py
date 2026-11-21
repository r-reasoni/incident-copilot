"""Start and supervise the local testbed services as plain Python processes."""
from __future__ import annotations

import multiprocessing
import time

import httpx


def _run_api() -> None:
    import uvicorn
    from testbed.api_service import app

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=8101, log_level="warning"))
    app.state.uvicorn_server = server
    server.run()


def _run_worker() -> None:
    import uvicorn
    from testbed.worker_service import app

    server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=8102, log_level="warning"))
    app.state.uvicorn_server = server
    server.run()


def main() -> None:
    processes = [
        multiprocessing.Process(target=_run_api, name="testbed-api"),
        multiprocessing.Process(target=_run_worker, name="testbed-worker"),
    ]
    for process in processes:
        process.start()
    print("Testbed services started: API http://127.0.0.1:8101, worker http://127.0.0.1:8102")
    try:
        while any(process.is_alive() for process in processes):
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("Stopping testbed services...")
    finally:
        for port in (8101, 8102):
            try:
                httpx.post(f"http://127.0.0.1:{port}/_internal/stop", timeout=0.5)
            except httpx.HTTPError:
                pass
        for process in processes:
            if process.is_alive():
                process.join(timeout=8)
        for process in processes:
            if process.is_alive():
                process.terminate()
            process.join(timeout=3)
        for process in processes:
            if process.is_alive():
                process.kill()
                process.join(timeout=2)
        print("Testbed services stopped.")


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()