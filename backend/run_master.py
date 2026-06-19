"""Start the Hydra master (REST API + scheduler + health monitor).

Usage:
    python run_master.py            # listens on 127.0.0.1:8000
    HYDRA_MASTER_PORT=9000 python run_master.py
"""
import uvicorn

from hydra import config

if __name__ == "__main__":
    print(f"Hydra master starting on {config.MASTER_URL}")
    uvicorn.run("hydra.master:app", host=config.MASTER_HOST, port=config.MASTER_PORT, log_level="warning")
