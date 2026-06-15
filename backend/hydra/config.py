import os

# Networking
MASTER_HOST = os.environ.get("HYDRA_MASTER_HOST", "127.0.0.1")
MASTER_PORT = int(os.environ.get("HYDRA_MASTER_PORT", "8000"))
MASTER_URL = f"http://{MASTER_HOST}:{MASTER_PORT}"

#Heartbeats / failure detection
# Worker emits a heartbeat every HEARTBEAT_INTERVAL seconds.
HEARTBEAT_INTERVAL = float(os.environ.get("HYDRA_HEARTBEAT_INTERVAL", "2.0"))
# Master declares a worker DEAD if no heartbeat arrives within this window.
HEARTBEAT_TIMEOUT = float(os.environ.get("HYDRA_HEARTBEAT_TIMEOUT", "6.0"))
# A task running longer than this (wall clock) is considered stuck and re-queued.
TASK_TIMEOUT = float(os.environ.get("HYDRA_TASK_TIMEOUT", "45.0"))

#Master loops
SCHEDULER_TICK = 0.4   # how often the scheduler tries to place work
MONITOR_TICK = 1.0     # how often the health monitor runs

DATA_ROOT = os.environ.get(
    "HYDRA_DATA_ROOT",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".hydra_data"),
)
OUTPUT_DIR = os.path.join(DATA_ROOT, "output")

# Retries
MAX_ATTEMPTS = int(os.environ.get("HYDRA_MAX_ATTEMPTS", "5"))


TASK_DELAY = float(os.environ.get("HYDRA_TASK_DELAY", "0.0"))
