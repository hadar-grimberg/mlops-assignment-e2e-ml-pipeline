import os
import json
import tarfile
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models.param import Param
from airflow.utils.context import Context
from docker.types import Mount


# Core path parameters mapped on the host machine
HOST_PROJECT_ROOT = Path(os.getenv("HOST_PROJECT_ROOT", "/home/ubuntu/mlops-assignment-e2e-ml-pipeline"))
HOST_RUNS_DIR = HOST_PROJECT_ROOT / "runs"

# Local Container Execution Paths (mapped inside our tasks)
CONTAINER_PROJECT_ROOT = Path("/opt/airflow")
CONTAINER_RUNS_DIR = CONTAINER_PROJECT_ROOT / "runs"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,                           # Phase 3 requirement: Add resilient task retries
    "retry_delay": timedelta(minutes=2),     # Grace period before spin retry
}

# --- Python Tasks ---

def summarize_and_log_task(**context: Context):
    """Step 4: Collect, organize structural manifest files and transfer outputs to remote S3 storage."""
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="prepare_run")
    container_run_dir = CONTAINER_RUNS_DIR / run_id
    with open(container_run_dir / "config.json", "r") as f:
        run_config = json.load(f)

    # 1. Metrics evaluation parse check
    summary_path = container_run_dir / "run-eval" / "reports" / "summary.json"
    metrics = {"resolved_count": 0, "total_count": 0, "resolved_rate": 0.0}
    if summary_path.exists():
        with open(summary_path, "r") as f:
            metrics = json.load(f)

    # 1b. Cost & trajectory metrics, aggregated and per-instance
    unresolved_instances = set(metrics.get("unresolved_instances", []))
    trajectories_dir = container_run_dir / "run-agent" / "trajectories"
    per_instance_metrics = {}
    total_cost = 0.0
    total_steps = 0
    instance_count = 0
    if trajectories_dir.exists():
        for traj_file in sorted(trajectories_dir.glob("*.json")):
            with open(traj_file, "r") as f:
                trajectory = json.load(f)
            instance_id = trajectory.get("instance_id", traj_file.stem)
            cost = trajectory.get("total_cost", 0.0)
            steps = len(trajectory.get("history", []))
            total_cost += cost
            total_steps += steps
            instance_count += 1
            per_instance_metrics[f"resolved.{instance_id}"] = 0.0 if instance_id in unresolved_instances else 1.0
            per_instance_metrics[f"cost.{instance_id}"] = cost
            per_instance_metrics[f"steps.{instance_id}"] = steps

    metrics["total_cost"] = round(total_cost, 4)
    metrics["avg_cost_per_instance"] = round(total_cost / instance_count, 4) if instance_count else 0.0
    metrics["avg_steps_per_instance"] = round(total_steps / instance_count, 4) if instance_count else 0.0

    with open(container_run_dir / "metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    # 2. Package Compress and Transfer up to remote long-term storage
    s3_uri = "local-only"
    s3_bucket = os.getenv("S3_BUCKET_NAME")
    
    if s3_bucket:
        try:
            import boto3
            tarball_path = CONTAINER_RUNS_DIR / f"{run_id}.tar.gz"
            with tarfile.open(tarball_path, "w:gz") as tar:
                tar.add(container_run_dir, arcname=run_id)

            s3_client = boto3.client(
                "s3",
                endpoint_url=os.getenv("S3_ENDPOINT_URL", "https://storage.api.nebius.ai"),
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
            )
            s3_key = f"mlops-assignments/runs/{run_id}.tar.gz"
            s3_client.upload_file(str(tarball_path), s3_bucket, s3_key)
            s3_uri = f"s3://{s3_bucket}/{s3_key}"
            tarball_path.unlink()
        except Exception as e:
            print(f"Long-term Object Storage transfer issue encountered: {e}")

    # 3. Structural Manifest validation
    manifest = {
        "run_id": run_id,
        "remote_artifact_uri": s3_uri,
        "artifacts": {
            "config": "config.json",
            "predictions": "run-agent/preds.json",
            "trajectories": "run-agent/trajectories/",
            "eval_logs": "run-eval/logs/",
            "eval_reports": "run-eval/reports/",
            "metrics": "metrics.json"
        }
    }
    with open(container_run_dir / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)

    # 4. Log params, metrics, and local artifact files directly into MLflow Tracking instance
    try:
        import mlflow
        mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000"))
        mlflow.set_experiment("polished-production-agent-evaluation")

        with mlflow.start_run(run_name=run_id):
            mlflow.log_params(run_config)
            numeric_metrics = {k: v for k, v in metrics.items() if isinstance(v, (int, float))}
            mlflow.log_metrics(numeric_metrics)
            if per_instance_metrics:
                mlflow.log_metrics(per_instance_metrics)
            mlflow.log_param("remote_storage_uri", s3_uri)
            mlflow.log_artifacts(str(container_run_dir), artifact_path="reproducible_run")
            print("Successfully recorded execution trace matrix into MLflow Registry Workspace.")
    except Exception as e:
        print(f"MLflow service unavailable: {e}")

# --- Airflow Core Pipeline Workflows Graph ---

with DAG(
    dag_id="evaluate_agent_production",
    default_args=default_args,
    description="Phase 3 Production Polish: Isolated Docker Pipelines for Coding Agents",
    schedule=None,
    catchup=False,
    params={
        "split": Param("lite", type="string"),
        "subset": Param("test", type="string"),
        "workers": Param(4, type="integer"),
        "model": Param("gpt-4o", type="string"),
        "task_slice": Param("0:1", type="string"),
        "run_id": Param("", type="string"),
        "cost_limit": Param(10.0, type="number"),
    }
) as dag:

    # Step 1 Polished: Isolated Container creation of the shared run workspace
    prepare_run = DockerOperator(
        task_id="prepare_run",
        image="mini-swe-agent-pipeline:latest",
        api_version="auto",
        auto_remove='success',
        network_mode="host",
        do_xcom_push=True,
        xcom_all=False,  # only the last stdout line (the run_id) is pushed to XCom
        mounts=[
            Mount(
                source=str(HOST_RUNS_DIR),
                target="/opt/airflow/runs",
                type="bind",
            )
        ],
        command=(
            "python scripts/prepare_run.py "
            "--run-id '{{ params.run_id }}' "
            "--split {{ params.split }} "
            "--subset {{ params.subset }} "
            "--workers {{ params.workers }} "
            "--model {{ params.model }} "
            "--task-slice {{ params.task_slice }} "
            "--cost-limit {{ params.cost_limit }}"
        )
    )

    # Step 2 Polished: Isolated Container execution of Agent Loop via DockerOperator
    # dynamically binds runtime configs to the container image
    run_agent = DockerOperator(
        task_id="run_agent",
        image="mini-swe-agent-pipeline:latest", # Built using provided assignment Dockerfile
        api_version="auto",
        auto_remove='success',
        network_mode="host",                     # Allows API communication and networking loops
        execution_timeout=timedelta(hours=2),    # Enforces an execution safety limit cutoff budget
        environment={
            "NEBIUS_API_KEY": os.getenv("NEBIUS_API_KEY", ""),
            "OPENAI_API_KEY": os.getenv("OPENAI_API_KEY", "")
        },
        # Mount the host runs directory path target into the container boundary
        mounts=[
            Mount(
                source=f"{HOST_RUNS_DIR}/{{{{ ti.xcom_pull(task_ids='prepare_run') }}}}",
                target="/opt/airflow/current_run",
                type="bind",
            )
        ],
        command=(
            "bash scripts/mini-swe-bench-batch.sh "
            "--split {{ params.split }} "
            "--subset {{ params.subset }} "
            "--workers {{ params.workers }} "
            "--model {{ params.model }} "
            "--task-slice {{ params.task_slice }} "
            "--cost-limit {{ params.cost_limit }} "
            "--output-dir /opt/airflow/current_run/run-agent"
        )
    )

    # Step 3 Polished: Isolated Container evaluation execution via DockerOperator
    run_eval = DockerOperator(
        task_id="run_eval",
        image="mini-swe-agent-pipeline:latest",
        api_version="auto",
        auto_remove='success',
        network_mode="host",
        execution_timeout=timedelta(hours=1),
        mounts=[
            Mount(
                source=f"{HOST_RUNS_DIR}/{{{{ ti.xcom_pull(task_ids='prepare_run') }}}}",
                target="/opt/airflow/current_run",
                type="bind",
            )
        ],
        command=(
            "bash scripts/swe-bench-eval.sh "
            "--preds /opt/airflow/current_run/run-agent/preds.json "
            "--output-dir /opt/airflow/current_run/run-eval"
        )
    )

    summarize_and_log = PythonOperator(
        task_id="summarize_and_log",
        python_callable=summarize_and_log_task,
    )

    # DAG Functional execution dependency line flow
    prepare_run >> run_agent >> run_eval >> summarize_and_log
