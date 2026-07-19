# 🏗️ Data Engineering Project — Month 1 Foundations

End-to-end streaming data pipeline for e-commerce transactions using Kafka, PySpark, Delta Lake, and Airflow.

---

## Architecture

```
┌──────────────────┐     ┌───────────┐     ┌─────────────────────┐     ┌────────────┐
│  Producer (Py)   │────▶│   Kafka   │────▶│  PySpark Streaming  │────▶│ Delta Lake │
│  scripts/        │     │  broker   │     │  scripts/consumer   │     │  delta/    │
│  producer.py     │     │  :29092   │     │                     │     │  output/   │
└──────────────────┘     └───────────┘     └─────────┬───────────┘     └────────────┘
                                                     │
                                           ┌─────────▼───────────┐
                                           │   Airflow DAGs      │
                                           │   (orchestration)   │
                                           │   dags/             │
                                           └─────────────────────┘
```

---

## Stack

| Component       | Version              |
|-----------------|----------------------|
| Python          | 3.12                 |
| PySpark         | 3.5.1                |
| Delta Lake      | 3.1.0                |
| Kafka           | 7.3.0 (Confluent)    |
| Airflow         | 2.9.1                |
| Pandas          | 2.2.2                |
| Docker Compose  | v2                   |

---

## Project Structure

```
data-engineering-project/
├── dags/
│   ├── dag_01_hello_world.py            # BashOperator + XCom demo
│   ├── dag_02_ecommerce_etl.py          # ETL pipeline + quality gate
│   └── dag_03_data_quality_branching.py # BranchPythonOperator routing
├── scripts/
│   ├── producer.py                      # Kafka JSON producer (partitioned by user_id)
│   └── consumer.py                      # PySpark Structured Streaming → Delta Lake
├── delta/                               # Generated — not committed
│   ├── output/                          # Delta table (parquet + _delta_log)
│   └── checkpoints/                     # Spark streaming checkpoints
├── logs/                                # Airflow logs — not committed
├── config/                              # Airflow config directory
├── plugins/                             # Airflow plugins directory
├── docker-compose.yaml                  # Airflow stack (CeleryExecutor + Postgres + Redis)
├── docker-compose-kafka.yml             # Kafka + Zookeeper + Kafka UI
├── .env                                 # AIRFLOW_UID (not committed)
├── .env.example                         # Environment variable template
├── requirements.txt                     # Python dependencies
└── README.md
```

---

## Setup

### Prerequisites

- Docker Desktop running
- Python 3.12+ installed locally
- Java 8 or 11 (for PySpark)
- Hadoop `winutils.exe` in `HADOOP_HOME` (Windows only)

### 1. Start Kafka

```bash
docker compose -f docker-compose-kafka.yml up -d
```

Kafka UI available at [http://localhost:8081](http://localhost:8081).

### 2. Start Airflow

```bash
echo "AIRFLOW_UID=50000" > .env
docker compose up -d
```

Airflow UI available at [http://localhost:8080](http://localhost:8080) (user: `airflow`, password: `airflow`).

### 3. Run the Kafka Producer

```bash
python scripts/producer.py
```

Sends mock e-commerce transactions to the `transactions_ecommerce` topic every 2 seconds.

### 4. Run the Spark Consumer

Open a second terminal:

```bash
python scripts/consumer.py
```

Reads from Kafka, parses JSON, adds `processing_timestamp`, writes to Delta Lake at `delta/output/`.

### 5. Reset Delta Lake (after schema changes)

```bash
# Windows (Admin PowerShell)
Remove-Item -Recurse -Force delta\checkpoints, delta\output

# Linux / macOS
rm -rf delta/checkpoints delta/output
```

---

## Phase Completion

| Phase                    | Status   | Progress |
|--------------------------|----------|----------|
| Phase 1 — Airflow DAGs   | ✅ Done  | 100%     |
| Phase 2 — Kafka          | ✅ Done  | 100%     |
| Phase 3 — Delta Lake     | ✅ Done  | 95%      |
| Phase 4 — Full Pipeline  | 🔴 TODO | 0%       |

---

## Environment Variables

Copy `.env.example` to `.env` and edit as needed:

| Variable                  | Default                                                    | Description                        |
|---------------------------|------------------------------------------------------------|------------------------------------|
| `HADOOP_HOME`             | `./hadoop`                                                 | Path to Hadoop binaries            |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092`                                          | Kafka broker address               |
| `KAFKA_TOPIC`             | `transactions_ecommerce`                                   | Topic name for transactions        |
| `DELTA_OUTPUT_PATH`       | `./delta/output`                                           | Delta table output directory       |
| `DELTA_CHECKPOINT_PATH`   | `./delta/checkpoints`                                      | Spark streaming checkpoint dir     |
| `SPARK_KAFKA_PACKAGE`     | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`        | Spark-Kafka connector Maven coord  |
| `DELTA_SPARK_PACKAGE`     | `io.delta:delta-spark_2.12:3.1.0`                          | Delta-Spark connector Maven coord  |
| `AIRFLOW_UID`             | `50000`                                                    | Linux UID for Airflow containers   |

---

## Author

**Abdelhakim Mahdad** — Data Engineering Learning Path, Month 1.
