"""Shared wire protocol: enums and request/response message shapes.

The master and workers only ever exchange JSON over HTTP. Keeping every
message defined here makes the contract between the two roles explicit and
easy to document for an interview.
"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


# --------------------------------------------------------------------------
# State machines
# --------------------------------------------------------------------------
class TaskState(str, Enum):
    PENDING = "PENDING"      # waiting to be scheduled
    RUNNING = "RUNNING"      # assigned to a worker, in flight
    SUCCESS = "SUCCESS"      # completed and result accepted
    FAILED = "FAILED"        # exhausted all retry attempts
    RETRYING = "RETRYING"    # failed once, queued for another attempt


class WorkerStatus(str, Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    DEAD = "DEAD"


class Phase(str, Enum):
    IDLE = "IDLE"        # no job submitted
    MAP = "MAP"          # map tasks in flight
    REDUCE = "REDUCE"    # shuffle + reduce in flight
    DONE = "DONE"        # final output written


class TaskKind(str, Enum):
    MAP = "map"
    REDUCE = "reduce"


# --------------------------------------------------------------------------
# Worker -> Master
# --------------------------------------------------------------------------
class RegisterRequest(BaseModel):
    worker_id: str
    url: str


class HeartbeatRequest(BaseModel):
    worker_id: str
    status: WorkerStatus
    current_task: Optional[str] = None


class TaskCompleteRequest(BaseModel):
    worker_id: str
    task_id: str
    attempt: int
    ok: bool
    error: Optional[str] = None
    # map result: number of distinct keys emitted; reduce result: output path
    keys: Optional[int] = None
    output_path: Optional[str] = None


# --------------------------------------------------------------------------
# Master -> Worker
# --------------------------------------------------------------------------
class MapLocation(BaseModel):
    """Where a completed map task's partition files can be fetched."""
    worker_id: str
    url: str
    map_task_id: str


class AssignRequest(BaseModel):
    kind: TaskKind
    task_id: str
    attempt: int
    num_reduce: int
    # map fields
    chunk_index: Optional[int] = None
    text: Optional[str] = None
    # reduce fields
    reduce_id: Optional[int] = None
    map_locations: Optional[List[MapLocation]] = None


# --------------------------------------------------------------------------
# Client -> Master
# --------------------------------------------------------------------------
class JobRequest(BaseModel):
    text: Optional[str] = None       # inline text
    dataset: Optional[str] = None    # name of a bundled sample dataset
    num_map: int = 4
    num_reduce: int = 2
