# Evaluation Pipeline — Report

## Architecture

The pipeline is an Airflow DAG (`dags/evaluate_agent.py`, `dag_id=evaluate_agent_production`) with four sequential tasks, each isolated in its own `mini-swe-agent-pipeline:latest` container (built from the root `Dockerfile`) via `DockerOperator`:

```
prepare_run  ─▶  run_agent  ─▶  run_eval  ─▶  summarize_and_log
(DockerOperator) (DockerOperator) (DockerOperator) (PythonOperator)
```

- **`prepare_run`** (`scripts/prepare_run.py`) reads the Airflow params (`split`, `subset`, `workers`, `model`, `task_slice`, `run_id`, `cost_limit`), generates a `run_id` if one wasn't supplied, creates the `runs/<run-id>/` skeleton, and writes `config.json`. It pushes `run_id` to XCom so downstream tasks can locate the run directory. It sets `umask(0o002)` so the directories it creates stay group-writable — this is what lets the later `summarize_and_log` step (running as the Airflow worker's own uid/gid) write into a directory a Docker container created as `root`.
- **`run_agent`** (`scripts/mini-swe-bench-batch.sh`) simulates a mini-swe-agent batch run: it selects a slice of a representative SWE-bench-lite instance pool according to `task_slice`, and for each instance deterministically derives a cost and step count from a hash of `(instance_id, model)` — so different `model`/`task_slice` params produce different `preds.json` and `run-agent/trajectories/*.json`, but the same params always reproduce the same output.
- **`run_eval`** (`scripts/swe-bench-eval.sh`) reads the `preds.json` that `run_agent` produced and derives a resolved/unresolved verdict per instance (deterministic hash, ~70% resolve rate), writing `run-eval/reports/summary.json` and `run-eval/logs/evaluation_run.log`.
- **`summarize_and_log`** (`summarize_and_log_task` in the DAG) parses `summary.json` and the trajectory files, computes aggregate metrics (`total_cost`, `avg_cost_per_instance`, `avg_steps_per_instance`) plus per-instance metrics (`resolved.<id>`, `cost.<id>`, `steps.<id>`), writes `metrics.json` and `manifest.json`, optionally uploads the run folder to S3 (falls back to `"local-only"` if `S3_BUCKET_NAME` isn't set), and logs params/metrics/artifact URI to MLflow.

Both `scripts/mini-swe-bench-batch.sh` and `scripts/swe-bench-eval.sh` are **mocked stand-ins** for the real mini-swe-agent / SWE-bench harness invocations (see the comments in each script for what the real commands would look like) — they don't actually spin up SWE-bench Docker environments or call an LLM. The orchestration, artifact layout, retries, isolation, and MLflow logging around them are real.

Airflow (`CeleryExecutor`) and MLflow run locally via `docker-compose.yaml` (Postgres + Redis + Airflow apiserver/scheduler/dag-processor/worker/triggerer + MLflow server with proxied artifact storage).

## How to trigger the DAG

1. Bring up the stack: `docker compose up -d` (from the repo root; requires `.env` with `NEBIUS_API_KEY` etc., and `AIRFLOW_UID` set — see README "Prerequisites").
2. Build the pipeline image (needed once, and again any time `scripts/` or the `Dockerfile` change): `docker build -t mini-swe-agent-pipeline:latest .`
3. Open the Airflow UI at `http://localhost:8080` (default login `airflow`/`airflow`), unpause `evaluate_agent_production`, and trigger it with your desired params (`split`, `subset`, `workers`, `model`, `task_slice`, `run_id`, `cost_limit`).
4. Or trigger from the CLI:
   ```
   docker exec <airflow-scheduler-container> airflow dags trigger evaluate_agent_production \
     --conf '{"split": "lite", "subset": "test", "model": "claude-sonnet-5", "task_slice": "3:8", "cost_limit": 10.0}'
   ```

## Artifact layout

Every run produces a self-contained folder:

```
runs/<run-id>/
  config.json          # resolved params for this run
  run-agent/
    preds.json          # instance_id -> patch, one entry per selected SWE-bench instance
    trajectories/        # one trajectory JSON per instance (model, cost, step history, status)
  run-eval/
    logs/evaluation_run.log
    reports/summary.json # resolved_count, total_count, resolved_rate, unresolved_instances
  metrics.json          # summary.json + total_cost / avg_cost_per_instance / avg_steps_per_instance
  manifest.json         # points to every artifact above + remote_artifact_uri (S3 URI or "local-only")
```

Sending someone the `runs/<run-id>/` folder (or its S3 URI, once remote storage is wired up) is enough to reconstruct the whole run: which params produced it, what the agent did, and how it scored.

## MLflow

- Tracking UI: `http://localhost:5000` (forward port 5000 if running on a remote VM).
- Experiment: `polished-production-agent-evaluation`.
- Each run is logged under its `run_id` as the MLflow run name, with:
  - **Params**: everything in `config.json` (`split`, `subset`, `workers`, `model`, `task_slice`, `cost_limit`, `run_id`, `timestamp`) plus `remote_storage_uri`.
  - **Metrics**: `resolved_count`, `total_count`, `resolved_rate`, `total_cost`, `avg_cost_per_instance`, `avg_steps_per_instance`, and per-instance `resolved.<instance_id>` / `cost.<instance_id>` / `steps.<instance_id>`.
  - **Artifacts**: the full `runs/<run-id>/` folder, logged under `reproducible_run/`.

*(No screenshot is checked into this repo — open the URL above after a run to see the experiment table. `S3_BUCKET_NAME`/`AWS_*` env vars aren't set in this environment, so `remote_storage_uri` currently logs as `"local-only"`; wiring real S3 credentials into `.env` is enough to activate the upload path in `summarize_and_log_task` without further code changes.)*

## One completed run

`run_20260708_223023`, triggered with `{"task_slice": "3:8", "model": "claude-sonnet-5"}`:

- `config.json`: `split=lite`, `subset=test`, `workers=4`, `model=claude-sonnet-5`, `task_slice=3:8`, `cost_limit=10.0`
- `metrics.json`:
  ```json
  {
    "resolved_count": 3,
    "total_count": 5,
    "resolved_rate": 60.0,
    "unresolved_instances": ["astropy__astropy-13579", "astropy__astropy-14096"],
    "total_cost": 3.0376,
    "avg_cost_per_instance": 0.6075,
    "avg_steps_per_instance": 2.4
  }
  ```
- MLflow run: `4125eacc70d74253b26a4f9541564ab6` (run name `run_20260708_223023`, experiment `polished-production-agent-evaluation`, status `FINISHED`).
- All four tasks (`prepare_run`, `run_agent`, `run_eval`, `summarize_and_log`) succeeded.

## Rerun instructions

To reproduce this exact run: trigger the DAG with the same `conf` above (`task_slice=3:8`, `model=claude-sonnet-5`, everything else default) — `run_agent` and `run_eval` are deterministic given the same params, so the same 5 instances, costs, step counts, and resolved/unresolved split will come back.

To run a different experiment, change any of `split`, `subset`, `workers`, `model`, `task_slice`, `cost_limit` in the trigger `conf`; a fresh `run_id` (timestamp-based, unless you pass one explicitly) will be generated, and the new run will show up alongside prior runs in both `runs/` and the MLflow experiment table for comparison.
