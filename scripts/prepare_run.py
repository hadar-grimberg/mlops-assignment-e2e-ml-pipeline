#!/usr/bin/env python3
"""Creates the shared run workspace and config.json for a pipeline run.

Runs inside the pipeline container with the host runs directory mounted at
/opt/airflow/runs. Prints the resolved run_id as the only stdout line so
Airflow's DockerOperator can pick it up via XCom.
"""
import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

RUNS_DIR = Path("/opt/airflow/runs")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", default="")
    parser.add_argument("--split", required=True)
    parser.add_argument("--subset", required=True)
    parser.add_argument("--workers", type=int, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--task-slice", required=True)
    parser.add_argument("--cost-limit", type=float, required=True)
    return parser.parse_args()


def main() -> None:
    os.umask(0o002)  # keep the run dir group-writable so other containers/uids can write into it
    args = parse_args()
    run_id = args.run_id or f"run_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    run_dir = RUNS_DIR / run_id

    (run_dir / "run-agent" / "trajectories").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval" / "logs").mkdir(parents=True, exist_ok=True)
    (run_dir / "run-eval" / "reports").mkdir(parents=True, exist_ok=True)

    run_config = {
        "run_id": run_id,
        "split": args.split,
        "subset": args.subset,
        "workers": args.workers,
        "model": args.model,
        "task_slice": args.task_slice,
        "cost_limit": args.cost_limit,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    with open(run_dir / "config.json", "w") as f:
        json.dump(run_config, f, indent=2)

    print(run_id)


if __name__ == "__main__":
    main()
