"""Hydra Worker.

A worker is a self-contained process that:
  * registers with the master and sends periodic heartbeats
  * accepts a pushed task (map or reduce) and runs it in a background thread
  * for MAP: counts words in its chunk and writes one partition file per
    reducer to its node-local disk
  * for REDUCE: fetches its partition from every map worker (the file-based
    shuffle), merges + sorts, and writes the final output file
  * serves its partition files to reducers over HTTP

Run many of these in separate terminals to form a cluster.
"""
from __future__ import annotations

import os
import threading
import time
from contextlib import asynccontextmanager

import requests
from fastapi import FastAPI, Response

from . import config
from .mapreduce import dump_pairs, load_pairs, map_chunk, partition_counts, reduce_pairs
from .protocol import AssignRequest, TaskKind, WorkerStatus


class Worker:
    def __init__(self, worker_id: str, port: int, master_url: str) -> None:
        self.worker_id = worker_id
        self.port = port
        self.master_url = master_url.rstrip("/")
        self.url = f"http://{config.MASTER_HOST}:{port}"
        self.data_dir = os.path.join(config.DATA_ROOT, worker_id)
        self.current_task = None  # task_id currently running (for heartbeat)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        os.makedirs(self.data_dir, exist_ok=True)
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)

    # ---- registration + heartbeats --------------------------------------
    def register(self) -> None:
        for _ in range(30):
            try:
                requests.post(
                    f"{self.master_url}/api/register",
                    json={"worker_id": self.worker_id, "url": self.url},
                    timeout=3,
                )
                print(f"[{self.worker_id}] registered with master at {self.master_url}")
                return
            except Exception:
                time.sleep(1)
        print(f"[{self.worker_id}] WARNING: could not reach master to register")

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            with self._lock:
                ct = self.current_task
            status = WorkerStatus.BUSY if ct else WorkerStatus.IDLE
            try:
                requests.post(
                    f"{self.master_url}/api/heartbeat",
                    json={
                        "worker_id": self.worker_id,
                        "status": status.value,
                        "current_task": ct,
                    },
                    timeout=3,
                )
            except Exception:
                pass  # transient; master will time us out if it persists
            time.sleep(config.HEARTBEAT_INTERVAL)

    def start_background(self) -> None:
        self.register()
        threading.Thread(target=self._heartbeat_loop, daemon=True, name="heartbeat").start()

    def stop(self) -> None:
        self._stop.set()

    # ---- task handling ---------------------------------------------------
    def handle_assignment(self, req: AssignRequest) -> None:
        """Accept a task and run it on a background thread (returns fast)."""
        with self._lock:
            self.current_task = req.task_id
        threading.Thread(target=self._run_task, args=(req,), daemon=True).start()

    def _run_task(self, req: AssignRequest) -> None:
        try:
            if req.kind == TaskKind.MAP:
                keys = self._run_map(req)
            else:
                keys = self._run_reduce(req)
            self._report(req, ok=True, keys=keys)
        except Exception as exc:
            print(f"[{self.worker_id}] task {req.task_id} error: {exc}")
            self._report(req, ok=False, error=str(exc))
        finally:
            with self._lock:
                if self.current_task == req.task_id:
                    self.current_task = None

    def _run_map(self, req: AssignRequest) -> int:
        if config.TASK_DELAY:
            time.sleep(config.TASK_DELAY)
        counts = map_chunk(req.text or "")
        buckets = partition_counts(counts, req.num_reduce)
        out_dir = os.path.join(self.data_dir, "intermediate", req.task_id)
        os.makedirs(out_dir, exist_ok=True)
        for r in range(req.num_reduce):
            with open(os.path.join(out_dir, f"part-{r}.txt"), "w", encoding="utf-8") as fh:
                fh.write(dump_pairs(buckets[r]))
        print(f"[{self.worker_id}] MAP {req.task_id}: {len(counts)} keys -> {req.num_reduce} parts")
        return len(counts)

    def _run_reduce(self, req: AssignRequest) -> int:
        if config.TASK_DELAY:
            time.sleep(config.TASK_DELAY)
        merged = []
        for loc in (req.map_locations or []):
            url = f"{loc.url}/partition/{loc.map_task_id}/{req.reduce_id}"
            resp = requests.get(url, timeout=5)
            resp.raise_for_status()
            merged.extend(load_pairs(resp.text))
        final = reduce_pairs(merged)
        out_path = os.path.join(config.OUTPUT_DIR, f"part-{req.reduce_id}.txt")
        with open(out_path, "w", encoding="utf-8") as fh:
            fh.write(dump_pairs(final))
        print(f"[{self.worker_id}] REDUCE {req.task_id}: merged -> {len(final)} keys -> {out_path}")
        return len(final)

    def read_partition(self, map_task_id: str, reduce_id: int) -> str:
        path = os.path.join(self.data_dir, "intermediate", map_task_id, f"part-{reduce_id}.txt")
        if not os.path.exists(path):
            return ""
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read()

    def _report(self, req: AssignRequest, ok: bool, keys: int = None, error: str = None) -> None:
        try:
            requests.post(
                f"{self.master_url}/api/task_complete",
                json={
                    "worker_id": self.worker_id,
                    "task_id": req.task_id,
                    "attempt": req.attempt,
                    "ok": ok,
                    "keys": keys,
                    "error": error,
                },
                timeout=5,
            )
        except Exception as exc:
            print(f"[{self.worker_id}] could not report {req.task_id}: {exc}")


def create_app(worker: Worker) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        worker.start_background()
        yield
        worker.stop()

    app = FastAPI(title=f"Hydra Worker {worker.worker_id}", lifespan=lifespan)

    @app.post("/assign")
    def assign(req: AssignRequest):
        worker.handle_assignment(req)
        return {"accepted": True}

    @app.get("/partition/{map_task_id}/{reduce_id}")
    def partition(map_task_id: str, reduce_id: int):
        return Response(content=worker.read_partition(map_task_id, reduce_id),
                        media_type="text/plain")

    @app.get("/health")
    def health():
        return {"worker_id": worker.worker_id, "current_task": worker.current_task}

    return app
