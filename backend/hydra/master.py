"""Hydra Master.

Responsibilities:
  * accept a word-count job and split the input into logical chunks
  * keep an in-memory worker registry driven by heartbeats
  * schedule map then reduce tasks onto idle workers (push model)
  * detect dead workers (heartbeat timeout) and stuck tasks (task timeout)
  * re-queue work so a single worker failure never loses progress
  * expose everything to the React dashboard over a small REST API

All mutable cluster state lives behind a single lock. Outbound HTTP calls to
workers are always made *outside* the lock so a slow/dead worker can never
freeze the scheduler.
"""
from __future__ import annotations

import os
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import config
from .logbus import LogBus
from .mapreduce import load_pairs, reduce_pairs
from .protocol import (
    AssignRequest,
    HeartbeatRequest,
    JobRequest,
    MapLocation,
    Phase,
    RegisterRequest,
    TaskCompleteRequest,
    TaskKind,
    TaskState,
    WorkerStatus,
)

DATASETS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "..", "data")


# --------------------------------------------------------------------------
# In-memory records
# --------------------------------------------------------------------------
@dataclass
class Worker:
    worker_id: str
    url: str
    status: WorkerStatus = WorkerStatus.IDLE
    current_task: Optional[str] = None
    last_heartbeat: float = field(default_factory=time.time)
    registered_at: float = field(default_factory=time.time)


@dataclass
class Task:
    task_id: str
    kind: TaskKind
    state: TaskState = TaskState.PENDING
    attempt: int = 1
    worker_id: Optional[str] = None
    chunk_index: Optional[int] = None
    reduce_id: Optional[int] = None
    keys: Optional[int] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None


@dataclass
class Job:
    job_id: str
    input_name: str
    num_map: int
    num_reduce: int
    chunks: List[str]
    created_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    top_words: List[List] = field(default_factory=list)
    output_file: Optional[str] = None
    total_words: int = 0


# --------------------------------------------------------------------------
# Cluster: all coordination logic
# --------------------------------------------------------------------------
class Cluster:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.log = LogBus()
        self.workers: Dict[str, Worker] = {}
        self.tasks: Dict[str, Task] = {}
        self.job: Optional[Job] = None
        self.phase: Phase = Phase.IDLE
        self._stop = threading.Event()

    # ---- worker lifecycle ------------------------------------------------
    def register(self, req: RegisterRequest) -> None:
        with self.lock:
            existing = self.workers.get(req.worker_id)
            self.workers[req.worker_id] = Worker(
                worker_id=req.worker_id,
                url=req.url,
                status=WorkerStatus.IDLE,
                last_heartbeat=time.time(),
            )
            verb = "re-registered" if existing else "registered"
        self.log.success(f"Worker {req.worker_id} {verb} at {req.url}")

    def heartbeat(self, req: HeartbeatRequest) -> None:
        with self.lock:
            w = self.workers.get(req.worker_id)
            if w is None:
                # unknown (e.g. master restarted) -> treat as a fresh register
                self.workers[req.worker_id] = Worker(req.worker_id, url="", status=req.status)
                return
            was_dead = w.status == WorkerStatus.DEAD
            w.last_heartbeat = time.time()
            if was_dead:
                # a worker we'd written off came back; let the scheduler reuse it
                w.status = WorkerStatus.IDLE
                w.current_task = None
                self.log.success(f"Worker {req.worker_id} recovered and is back online")

    # ---- job submission --------------------------------------------------
    def submit_job(self, req: JobRequest) -> Dict:
        text, name = self._resolve_input(req)
        chunks = self._split_chunks(text, req.num_map)
        with self.lock:
            if self.phase in (Phase.MAP, Phase.REDUCE):
                return {"ok": False, "error": "a job is already running"}
            job_id = f"job-{int(time.time())}"
            self.job = Job(
                job_id=job_id,
                input_name=name,
                num_map=len(chunks),
                num_reduce=req.num_reduce,
                chunks=chunks,
            )
            self.tasks = {}
            for i in range(len(chunks)):
                tid = f"map-{i}"
                self.tasks[tid] = Task(tid, TaskKind.MAP, chunk_index=i)
            for r in range(req.num_reduce):
                tid = f"reduce-{r}"
                self.tasks[tid] = Task(tid, TaskKind.REDUCE, reduce_id=r)
            # free any busy workers from a previous run
            for w in self.workers.values():
                if w.status != WorkerStatus.DEAD:
                    w.status = WorkerStatus.IDLE
                    w.current_task = None
            self.phase = Phase.MAP
        self.log.info(
            f"Job {job_id} submitted: '{name}' -> {len(chunks)} map tasks, "
            f"{req.num_reduce} reduce tasks"
        )
        return {"ok": True, "job_id": job_id, "num_map": len(chunks), "num_reduce": req.num_reduce}

    def reset(self) -> None:
        with self.lock:
            self.job = None
            self.tasks = {}
            self.phase = Phase.IDLE
            for w in self.workers.values():
                if w.status != WorkerStatus.DEAD:
                    w.status = WorkerStatus.IDLE
                    w.current_task = None
        self.log.warn("Cluster reset — job state cleared")

    # ---- completion ------------------------------------------------------
    def task_complete(self, req: TaskCompleteRequest) -> Dict:
        with self.lock:
            t = self.tasks.get(req.task_id)
            if t is None:
                return {"ok": False, "error": "unknown task"}
            # Reject stale / duplicate results using (task_id, attempt). This is
            # what stops a slow worker that was already re-assigned from
            # corrupting the output with an out-of-date result.
            if req.attempt != t.attempt:
                self.log.warn(
                    f"Ignored stale result for {t.task_id} "
                    f"(attempt {req.attempt}, expected {t.attempt})"
                )
                return {"ok": False, "error": "stale attempt"}
            if t.state == TaskState.SUCCESS:
                self.log.warn(f"Ignored duplicate result for {t.task_id}")
                return {"ok": False, "error": "already complete"}

            w = self.workers.get(req.worker_id)
            if not req.ok:
                self.log.error(f"Task {t.task_id} failed on {req.worker_id}: {req.error}")
                self._requeue(t, f"worker reported error: {req.error}")
                if w:
                    w.status = WorkerStatus.IDLE
                    w.current_task = None
                return {"ok": True}

            t.state = TaskState.SUCCESS
            t.finished_at = time.time()
            t.keys = req.keys
            if w and w.current_task == t.task_id:
                w.status = WorkerStatus.IDLE
                w.current_task = None
            dur = (t.finished_at - t.started_at) if t.started_at else 0.0
            self.log.success(
                f"{t.kind.value.upper()} {t.task_id} done on {req.worker_id} "
                f"in {dur:.1f}s ({req.keys} keys)"
            )
        return {"ok": True}

    # ---- scheduler (called on a loop) -----------------------------------
    def scheduler_step(self) -> None:
        assignments: List[tuple] = []  # (Worker, AssignRequest)
        with self.lock:
            if self.job is None or self.phase in (Phase.IDLE, Phase.DONE):
                return
            self._advance_phase()
            if self.phase == Phase.DONE:
                return

            kind = TaskKind.MAP if self.phase == Phase.MAP else TaskKind.REDUCE
            idle = [w for w in self.workers.values() if w.status == WorkerStatus.IDLE]
            schedulable = [
                t for t in self.tasks.values()
                if t.kind == kind and t.state in (TaskState.PENDING, TaskState.RETRYING)
            ]
            for task in schedulable:
                if not idle:
                    break
                worker = idle.pop()
                # tentatively mark in-flight under the lock
                task.state = TaskState.RUNNING
                task.worker_id = worker.worker_id
                task.started_at = time.time()
                worker.status = WorkerStatus.BUSY
                worker.current_task = task.task_id
                assignments.append((worker, self._build_assign(task)))

        # network I/O happens outside the lock
        for worker, payload in assignments:
            self._push_assignment(worker, payload)

    def _build_assign(self, task: Task) -> AssignRequest:
        if task.kind == TaskKind.MAP:
            return AssignRequest(
                kind=TaskKind.MAP,
                task_id=task.task_id,
                attempt=task.attempt,
                num_reduce=self.job.num_reduce,
                chunk_index=task.chunk_index,
                text=self.job.chunks[task.chunk_index],
            )
        locations = [
            MapLocation(
                worker_id=t.worker_id,
                url=self.workers[t.worker_id].url,
                map_task_id=t.task_id,
            )
            for t in self.tasks.values()
            if t.kind == TaskKind.MAP and t.state == TaskState.SUCCESS and t.worker_id in self.workers
        ]
        return AssignRequest(
            kind=TaskKind.REDUCE,
            task_id=task.task_id,
            attempt=task.attempt,
            num_reduce=self.job.num_reduce,
            reduce_id=task.reduce_id,
            map_locations=locations,
        )

    def _push_assignment(self, worker: Worker, payload: AssignRequest) -> None:
        try:
            resp = requests.post(
                f"{worker.url}/assign", json=payload.model_dump(), timeout=5
            )
            resp.raise_for_status()
            self.log.info(
                f"Assigned {payload.task_id} (attempt {payload.attempt}) -> {worker.worker_id}"
            )
        except Exception as exc:  # worker unreachable -> treat as dead
            with self.lock:
                self.log.error(f"Failed to assign {payload.task_id} to {worker.worker_id}: {exc}")
                self._mark_dead(worker.worker_id, reason="unreachable during assignment")

    # ---- health monitor (called on a loop) ------------------------------
    def monitor_step(self) -> None:
        with self.lock:
            now = time.time()
            for w in list(self.workers.values()):
                if w.status == WorkerStatus.DEAD:
                    continue
                if now - w.last_heartbeat > config.HEARTBEAT_TIMEOUT:
                    self._mark_dead(
                        w.worker_id,
                        reason=f"no heartbeat for {now - w.last_heartbeat:.1f}s",
                    )
            # stuck tasks (worker alive but task ran too long)
            for t in self.tasks.values():
                if t.state == TaskState.RUNNING and t.started_at:
                    if now - t.started_at > config.TASK_TIMEOUT:
                        self.log.warn(f"Task {t.task_id} exceeded timeout — re-queuing")
                        w = self.workers.get(t.worker_id) if t.worker_id else None
                        if w and w.status != WorkerStatus.DEAD:
                            w.status = WorkerStatus.IDLE
                            w.current_task = None
                        self._requeue(t, "task timeout")

    # ---- internal helpers (assume lock held) ----------------------------
    def _mark_dead(self, worker_id: str, reason: str) -> None:
        w = self.workers.get(worker_id)
        if w is None or w.status == WorkerStatus.DEAD:
            return
        w.status = WorkerStatus.DEAD
        w.current_task = None
        self.log.error(f"Worker {worker_id} declared DEAD ({reason})")

        regressed = False
        for t in self.tasks.values():
            if t.worker_id != worker_id:
                continue
            if t.state == TaskState.RUNNING:
                self._requeue(t, f"worker {worker_id} died mid-task")
            elif t.state == TaskState.SUCCESS and t.kind == TaskKind.MAP:
                # The map output lived on that worker's local disk and is now
                # unreachable for the shuffle, so the map must be recomputed.
                self.log.warn(f"Map output for {t.task_id} lost with {worker_id} — recomputing")
                self._requeue(t, "map output lost")
                regressed = True

        if regressed and self.phase in (Phase.REDUCE, Phase.DONE):
            self.phase = Phase.MAP
            self.job.finished_at = None
            for rt in self.tasks.values():
                if rt.kind == TaskKind.REDUCE and rt.state != TaskState.PENDING:
                    rt.state = TaskState.PENDING
                    rt.attempt += 1
                    rw = self.workers.get(rt.worker_id) if rt.worker_id else None
                    if rw and rw.status == WorkerStatus.BUSY and rw.current_task == rt.task_id:
                        rw.status = WorkerStatus.IDLE
                        rw.current_task = None
                    rt.worker_id = None
                    rt.started_at = None
            self.log.warn("Regressed to MAP phase; reduce tasks reset")

    def _requeue(self, task: Task, reason: str) -> None:
        task.worker_id = None
        task.started_at = None
        task.finished_at = None
        task.keys = None
        if task.attempt >= config.MAX_ATTEMPTS:
            task.state = TaskState.FAILED
            self.log.error(f"Task {task.task_id} FAILED permanently after {task.attempt} attempts")
            return
        task.attempt += 1
        task.state = TaskState.RETRYING
        self.log.warn(f"Re-queued {task.task_id} (attempt {task.attempt}) — {reason}")

    def _advance_phase(self) -> None:
        if self.phase == Phase.MAP:
            maps = [t for t in self.tasks.values() if t.kind == TaskKind.MAP]
            if maps and all(t.state == TaskState.SUCCESS for t in maps):
                # also require every holder worker to still be alive
                holders_alive = all(
                    self.workers.get(t.worker_id) and
                    self.workers[t.worker_id].status != WorkerStatus.DEAD
                    for t in maps
                )
                if holders_alive:
                    self.phase = Phase.REDUCE
                    self.log.info("All map tasks complete — entering SHUFFLE + REDUCE phase")
        elif self.phase == Phase.REDUCE:
            reds = [t for t in self.tasks.values() if t.kind == TaskKind.REDUCE]
            if reds and all(t.state == TaskState.SUCCESS for t in reds):
                self._finalize()

    def _finalize(self) -> None:
        merged: List = []
        for r in range(self.job.num_reduce):
            path = os.path.join(config.OUTPUT_DIR, f"part-{r}.txt")
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as fh:
                    merged.extend(load_pairs(fh.read()))
        final = reduce_pairs(merged)  # already-summed parts; just sorts/merges
        os.makedirs(config.OUTPUT_DIR, exist_ok=True)
        final_path = os.path.join(config.OUTPUT_DIR, "final_output.txt")
        with open(final_path, "w", encoding="utf-8") as fh:
            for w, c in final:
                fh.write(f"{w}\t{c}\n")
        self.phase = Phase.DONE
        self.job.finished_at = time.time()
        self.job.output_file = final_path
        self.job.total_words = sum(c for _, c in final)
        self.job.top_words = [[w, c] for w, c in sorted(final, key=lambda x: -x[1])[:20]]
        self.log.success(
            f"Job complete — {len(final)} unique words, {self.job.total_words} total. "
            f"Output: {final_path}"
        )

    # ---- input handling --------------------------------------------------
    def _resolve_input(self, req: JobRequest) -> tuple:
        if req.text:
            return req.text, "inline-text"
        if req.dataset:
            path = os.path.join(DATASETS_DIR, req.dataset)
            with open(path, "r", encoding="utf-8") as fh:
                return fh.read(), req.dataset
        # default sample
        path = os.path.join(DATASETS_DIR, "sample.txt")
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read(), "sample.txt"

    @staticmethod
    def _split_chunks(text: str, n: int) -> List[str]:
        lines = text.splitlines(keepends=True)
        if not lines:
            return [""]
        n = max(1, min(n, len(lines)))
        size = len(lines) // n
        rem = len(lines) % n
        chunks, idx = [], 0
        for i in range(n):
            take = size + (1 if i < rem else 0)
            chunks.append("".join(lines[idx:idx + take]))
            idx += take
        return chunks

    # ---- snapshot for the UI --------------------------------------------
    def status(self) -> Dict:
        with self.lock:
            now = time.time()
            workers = [
                {
                    "worker_id": w.worker_id,
                    "url": w.url,
                    "status": w.status.value,
                    "current_task": w.current_task,
                    "last_heartbeat": w.last_heartbeat,
                    "seconds_since_hb": round(now - w.last_heartbeat, 1),
                    "registered_at": w.registered_at,
                }
                for w in sorted(self.workers.values(), key=lambda x: x.worker_id)
            ]
            tasks = [
                {
                    "task_id": t.task_id,
                    "kind": t.kind.value,
                    "state": t.state.value,
                    "attempt": t.attempt,
                    "worker_id": t.worker_id,
                    "chunk_index": t.chunk_index,
                    "reduce_id": t.reduce_id,
                    "keys": t.keys,
                    "duration": round((t.finished_at or now) - t.started_at, 1) if t.started_at else None,
                }
                for t in sorted(self.tasks.values(), key=lambda x: (x.kind.value, x.task_id))
            ]
            job = None
            if self.job:
                maps = [t for t in self.tasks.values() if t.kind == TaskKind.MAP]
                reds = [t for t in self.tasks.values() if t.kind == TaskKind.REDUCE]
                m_done = sum(1 for t in maps if t.state == TaskState.SUCCESS)
                r_done = sum(1 for t in reds if t.state == TaskState.SUCCESS)
                if self.phase in (Phase.IDLE, Phase.MAP):
                    shuffle = "PENDING"
                elif self.phase == Phase.REDUCE:
                    shuffle = "IN_PROGRESS"
                else:
                    shuffle = "DONE"
                job = {
                    "job_id": self.job.job_id,
                    "input_name": self.job.input_name,
                    "num_map": self.job.num_map,
                    "num_reduce": self.job.num_reduce,
                    "created_at": self.job.created_at,
                    "finished_at": self.job.finished_at,
                    "map": {
                        "total": len(maps),
                        "done": m_done,
                        "pct": round(100 * m_done / len(maps)) if maps else 0,
                    },
                    "reduce": {
                        "total": len(reds),
                        "done": r_done,
                        "pct": round(100 * r_done / len(reds)) if reds else 0,
                    },
                    "shuffle_status": shuffle,
                    "top_words": self.job.top_words,
                    "total_words": self.job.total_words,
                    "output_file": self.job.output_file,
                }
            counts = {"IDLE": 0, "BUSY": 0, "DEAD": 0}
            for w in self.workers.values():
                counts[w.status.value] += 1
            return {
                "now": now,
                "phase": self.phase.value,
                "cluster": {
                    "workers_total": len(self.workers),
                    "idle": counts["IDLE"],
                    "busy": counts["BUSY"],
                    "dead": counts["DEAD"],
                },
                "job": job,
                "workers": workers,
                "tasks": tasks,
            }

    # ---- background loops ------------------------------------------------
    def run_loops(self) -> None:
        def scheduler():
            while not self._stop.is_set():
                try:
                    self.scheduler_step()
                except Exception as exc:  # never let the loop die
                    self.log.error(f"scheduler error: {exc}")
                time.sleep(config.SCHEDULER_TICK)

        def monitor():
            while not self._stop.is_set():
                try:
                    self.monitor_step()
                except Exception as exc:
                    self.log.error(f"monitor error: {exc}")
                time.sleep(config.MONITOR_TICK)

        threading.Thread(target=scheduler, daemon=True, name="scheduler").start()
        threading.Thread(target=monitor, daemon=True, name="monitor").start()

    def stop(self) -> None:
        self._stop.set()


# --------------------------------------------------------------------------
# FastAPI app
# --------------------------------------------------------------------------
cluster = Cluster()


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    cluster.run_loops()
    cluster.log.info("Hydra master online")
    yield
    cluster.stop()


app = FastAPI(title="Hydra Master", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/api/register")
def api_register(req: RegisterRequest):
    cluster.register(req)
    return {"ok": True}


@app.post("/api/heartbeat")
def api_heartbeat(req: HeartbeatRequest):
    cluster.heartbeat(req)
    return {"ok": True}


@app.post("/api/task_complete")
def api_task_complete(req: TaskCompleteRequest):
    return cluster.task_complete(req)


@app.post("/api/job")
def api_job(req: JobRequest):
    return cluster.submit_job(req)


@app.post("/api/reset")
def api_reset():
    cluster.reset()
    return {"ok": True}


@app.get("/api/status")
def api_status():
    return cluster.status()


@app.get("/api/logs")
def api_logs(after: int = 0):
    return {"events": cluster.log.after(after)}


@app.post("/api/logs/clear")
def api_logs_clear():
    cluster.log.clear()
    return {"ok": True}


@app.get("/api/datasets")
def api_datasets():
    try:
        files = sorted(f for f in os.listdir(DATASETS_DIR) if f.endswith(".txt"))
    except FileNotFoundError:
        files = []
    return {"datasets": files}
