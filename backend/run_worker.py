"""Start a Hydra worker process.

Usage:
    python run_worker.py --id worker-1 --port 9001
    python run_worker.py --id worker-2 --port 9002 --master http://127.0.0.1:8000

Run several in separate terminals to form a cluster. Kill one (Ctrl+C) during
a job to watch the master detect the failure and reassign its work.
"""
import argparse

import uvicorn

from hydra import config
from hydra.worker import Worker, create_app


def main() -> None:
    parser = argparse.ArgumentParser(description="Hydra worker")
    parser.add_argument("--id", required=True, help="unique worker id, e.g. worker-1")
    parser.add_argument("--port", type=int, required=True, help="port for this worker's HTTP server")
    parser.add_argument("--master", default=config.MASTER_URL, help="master base URL")
    args = parser.parse_args()

    worker = Worker(worker_id=args.id, port=args.port, master_url=args.master)
    app = create_app(worker)
    print(f"Worker {args.id} starting on {worker.url} (master: {args.master})")
    uvicorn.run(app, host=config.MASTER_HOST, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
