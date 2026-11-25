# CorpTech Enterprise Lakehouse & Medallion Pipeline (Starter)

Student starter repository for the **Apache Spark Lakehouse & Medallion Architecture** course project.

- **Assignment Specification:** [`docs/STUDENT_ASSIGNMENT.md`](./docs/STUDENT_ASSIGNMENT.md)
- **14-Day Data Sanity Checks & KPI Glossary:** [`docs/SANITY_CHECKS.md`](./docs/SANITY_CHECKS.md)
- **Architecture Design Template (Student Deliverable):** [`docs/architecture.md`](./docs/architecture.md)
- **Operations Runbook (Student Deliverable):** [`docs/runbook.md`](./docs/runbook.md)

---

## Repository Layout

```text
spark-lakehouse-starter/
├── docker/
│   ├── airflow/Dockerfile        # Airflow 2.10.5 (LocalExecutor) + PySpark 3.5.5 + Delta JARs
│   ├── reporting/Dockerfile      # Streamlit + DuckDB (with pre-installed delta extension)
│   └── spark/Dockerfile          # Spark 3.5.5 + Delta Lake 3.3.0 + Hadoop-AWS 3.3.4 JARs
├── docker-compose.yml            # Reproducible stack (+ optional --profile minio)
├── pyproject.toml                # Python dependency specification & pytest/ruff config
├── .env.example                  # Environment variable template
├── scripts/
│   ├── download_data.sh          # Linux/macOS dataset download & verification script
│   └── download_data.ps1         # Windows PowerShell dataset download & verification script
├── dags/
│   └── medallion_daily_dag.py    # Parameterized Airflow DAG (`clients` wired + TODO tasks)
├── spark_jobs/
│   ├── common/spark_session.py   # Shared Delta/S3A SparkSession builder & path helpers
│   ├── bronze/                   # Reference `ingest_clients.py` + TODO skeletons
│   ├── silver/                   # Reference `transform_clients.py` + TODO skeletons
│   └── gold/                     # TODO skeletons for the 5 Gold marts
├── dq/
│   ├── framework.py              # `DQCheckResult`, `dq_audit_results` Delta writer, gate runner
│   └── checks.py                 # TODO skeletons for full Bronze/Silver/Gold DQ suite
├── reporting/
│   ├── db.py                     # DuckDB `delta_scan` helper
│   └── app.py                    # Streamlit Home status page + 4 TODO view tabs
├── tests/
│   ├── conftest.py               # PySpark local session fixture
│   └── test_clients_pipeline.py  # Working tests for reference `clients` Bronze/Silver & DQ audit
└── docs/
    ├── STUDENT_ASSIGNMENT.md
    ├── SANITY_CHECKS.md
    ├── architecture.md
    └── runbook.md
```

---

## Quickstart

### 1. Prerequisites
- **Docker Desktop** with **Docker Compose v2** (`docker compose`)
- *(Optional)* [`uv`](https://docs.astral.sh/uv/) for local linting/formatting without containers

> **Windows Note:** Running Spark and `pytest` inside the provided Docker containers avoids local Hadoop `winutils.exe` and JVM version issues.

### 2. Configure & Start the Local Stack
```bash
cp .env.example .env
docker compose up -d --build
```
*(To also start a local S3-compatible MinIO server, run `docker compose --profile minio up -d --build`.)*

### 3. Download & Verify the Landing Dataset
Use the link or S3/MinIO bucket provided by your instructor:

**macOS / Linux / Git Bash:**
```bash
./scripts/download_data.sh --url "<DATASET_DOWNLOAD_URL>"
```

**Windows (PowerShell):**
```powershell
.\scripts\download_data.ps1 -Url "<DATASET_DOWNLOAD_URL>"
```

*(Both scripts also support `--s3-bucket` / `-S3Bucket` and `--endpoint-url` / `-EndpointUrl`, verify `MANIFEST.sha256` if present, and confirm all 10 dataset folders exist under `./data/landing`.)*

### 4. Run the Reference `clients` Mini-Pipeline
```bash
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/workspace/spark_jobs/bronze/ingest_clients.py \
  --batch-date 2026-01-01

docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  /opt/workspace/spark_jobs/silver/transform_clients.py \
  --batch-date 2026-01-01
```

### 5. Run Unit Tests
```bash
docker compose run --rm spark-master pytest -v
```

---

## Local Service URLs

| Service | URL | Credentials |
| :--- | :--- | :--- |
| **Apache Airflow** | http://localhost:8080 | `admin` / `admin` |
| **Spark Master UI** | http://localhost:8081 | — |
| **Spark Worker UI** | http://localhost:8082 | — |
| **Spark History Server** | http://localhost:18080 | — |
| **Streamlit Reporting UI** | http://localhost:8501 | — |
| **MinIO Console** *(optional)* | http://localhost:9001 | `minioadmin` / `minioadmin` |

---

## What Is Provided vs. What You Must Implement

### Provided in Starter
- Dockerized **Spark 3.5.5 + Delta Lake 3.3.0 + Airflow 2.10.5 + DuckDB/Streamlit** environment (with JARs pre-baked for offline execution)
- Dataset download & verification scripts (`scripts/download_data.sh` & `.ps1`)
- Shared `SparkSession` builder (`spark_jobs/common/spark_session.py`)
- Reference Bronze → Silver mini-pipeline for **`clients`** (`ingest_clients.py` & `transform_clients.py`), `dq_audit_results` schema/writer (`dq/framework.py`), and unit tests (`tests/test_clients_pipeline.py`)
- Parameterized Airflow DAG skeleton (`dags/medallion_daily_dag.py`) and Streamlit overview skeleton (`reporting/app.py`)

### Student Deliverables (`TODO`)
1. **Bronze Ingestion:** Implement `ingest_reference.py`, `ingest_hr.py`, `ingest_finance.py`, and `ingest_crm_surveys.py` with idempotent `_batch_date` partitioning.
2. **Silver Transformations & Quarantine:** Implement `employees` SCD Type 2, `timesheets` dedup/quarantine, `fx_rates` calendar spine + LOCF, `invoices` & `expenses` EU date parsing + FX normalization + orphan quarantine, `crm_events` skew-mitigated join (with Spark UI evidence in `docs/architecture.md`), and `employee_surveys` explode + bounds checks.
3. **Gold Data Marts:** Implement all 5 Gold tables (`gold_project_profitability`, `gold_department_finance`, `gold_client_engagement`, `gold_hr_enps_analytics`, `gold_employee_churn_features`).
4. **Data Quality Gates:** Implement full Hard/Soft checks in `dq/checks.py` and wire `dq_*_gate` tasks in Airflow.
5. **Orchestration & Reporting:** Wire all tasks in `dags/medallion_daily_dag.py`, build the 4 views in `reporting/app.py`, add tests in `tests/`, and complete `docs/architecture.md` and `docs/runbook.md`.
