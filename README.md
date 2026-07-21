<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Kafka"/>
  <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Spark"/>
  <img src="https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge&logo=databricks&logoColor=white" alt="Delta Lake"/>
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white" alt="Airflow"/>
  <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"/>
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
</p>

<h1 align="center">🏗️ Data Engineering Project</h1>

<p align="center">
  <strong>End-to-end streaming data pipeline for e-commerce transactions</strong><br>
  <em>Month 1 — Foundations · Big Data Engineer Learning Path</em>
</p>

<p align="center">
  <a href="#-architecture">Architecture</a> •
  <a href="#-tech-stack">Tech Stack</a> •
  <a href="#-project-structure">Structure</a> •
  <a href="#-getting-started">Getting Started</a> •
  <a href="#-airflow-dags">DAGs</a> •
  <a href="#-progress">Progress</a>
</p>

---

## 🏛️ Architecture

```mermaid
flowchart LR
    subgraph INGESTION ["⚡ Ingestion"]
        P["🐍 Producer<br/><code>producer.py</code>"]
    end

    subgraph STREAMING ["📡 Streaming"]
        K["📨 Kafka<br/>Broker :29092"]
        KUI["🖥️ Kafka UI<br/>:8081"]
    end

    subgraph PROCESSING ["⚙️ Processing"]
        S["🔥 PySpark<br/>Structured Streaming"]
    end

    subgraph STORAGE ["💾 Storage"]
        D["🗂️ Delta Lake<br/>Parquet + Transaction Log"]
    end

    subgraph ORCHESTRATION ["🎯 Orchestration"]
        A["🌬️ Airflow<br/>3 DAGs · :8080"]
    end

    P -->|JSON events| K
    K -.->|monitoring| KUI
    K -->|subscribe| S
    S -->|append mode| D
    A -.->|schedules & monitors| S
    A -.->|quality checks| D

    style INGESTION fill:#1a1a2e,stroke:#e94560,color:#fff
    style STREAMING fill:#1a1a2e,stroke:#0f3460,color:#fff
    style PROCESSING fill:#1a1a2e,stroke:#e94560,color:#fff
    style STORAGE fill:#1a1a2e,stroke:#16213e,color:#fff
    style ORCHESTRATION fill:#1a1a2e,stroke:#0f3460,color:#fff
```

### Data Flow

```
🛒 E-commerce Transaction
│
├──▶ Producer generates JSON ──▶ Kafka topic: transactions_ecommerce
│                                       │
│                                       ▼
│                              PySpark reads stream
│                              ├── Parse JSON schema
│                              ├── Add processing_timestamp
│                              └── Write to Delta Lake (append)
│
└──▶ Airflow DAGs
     ├── DAG 01: Environment check + XCom demo
     ├── DAG 02: ETL pipeline + quality gate
     └── DAG 03: Branching based on data quality
```

---

## 🛠️ Tech Stack

| Component | Version | Role |
|:---------:|:-------:|:-----|
| <img src="https://img.shields.io/badge/-Python-3776AB?logo=python&logoColor=white&style=flat-square" /> | `3.12` | Core language |
| <img src="https://img.shields.io/badge/-PySpark-E25A1C?logo=apachespark&logoColor=white&style=flat-square" /> | `3.5.1` | Stream processing |
| <img src="https://img.shields.io/badge/-Delta_Lake-003366?logo=databricks&logoColor=white&style=flat-square" /> | `3.1.0` | ACID storage layer |
| <img src="https://img.shields.io/badge/-Kafka-231F20?logo=apachekafka&logoColor=white&style=flat-square" /> | `7.3.0` | Message broker (Confluent) |
| <img src="https://img.shields.io/badge/-Airflow-017CEE?logo=apacheairflow&logoColor=white&style=flat-square" /> | `2.9.1` | Workflow orchestration |
| <img src="https://img.shields.io/badge/-Pandas-150458?logo=pandas&logoColor=white&style=flat-square" /> | `2.2.2` | Data transformation |
| <img src="https://img.shields.io/badge/-Docker-2496ED?logo=docker&logoColor=white&style=flat-square" /> | `v2` | Containerization |

---

## 📁 Project Structure

```
data-engineering-project/
│
├── 📂 dags/                                  ← Airflow DAG definitions
│   ├── dag_01_hello_world.py                    BashOperator · XCom · Jinja
│   ├── dag_02_ecommerce_etl.py                  Extract → Transform → Quality Gate → Load
│   └── dag_03_data_quality_branching.py          BranchPythonOperator routing
│
├── 📂 scripts/                               ← Streaming pipeline scripts
│   ├── producer.py                              Kafka producer (JSON, partitioned by user_id)
│   └── consumer.py                              PySpark Structured Streaming → Delta Lake
│
├── 📂 delta/                                 ← Generated data (git-ignored)
│   ├── output/                                  Delta table (parquet + _delta_log/)
│   └── checkpoints/                             Spark streaming checkpoints
│
├── 📂 config/                                ← Airflow configuration
├── 📂 plugins/                               ← Airflow plugins
├── 📂 logs/                                  ← Airflow logs (git-ignored)
│
├── 🐳 docker-compose.yaml                   ← Airflow (CeleryExecutor + Postgres + Redis)
├── 🐳 docker-compose-kafka.yml              ← Kafka + Zookeeper + Kafka UI
├── 📄 requirements.txt                       ← Python dependencies (pinned)
├── 📄 .env.example                           ← Environment variable template
├── 📄 .gitignore                             ← Excludes delta/, logs/, .env, etc.
└── 📄 README.md
```

---

## 🚀 Getting Started

### Prerequisites

| Requirement | Why |
|-------------|-----|
| **Docker Desktop** | Runs Kafka, Airflow, Postgres, Redis |
| **Python 3.12+** | Producer & Spark scripts |
| **Java 8 or 11** | PySpark runtime |
| **Hadoop winutils** | Windows only — set `HADOOP_HOME` |

### 1️⃣ Clone & Configure

```bash
git clone https://github.com/MdAbdehakim/data-engineering-project.git
cd data-engineering-project
cp .env.example .env
```

### 2️⃣ Start Kafka

```bash
docker compose -f docker-compose-kafka.yml up -d
```

> **Kafka UI** → [http://localhost:8081](http://localhost:8081)

### 3️⃣ Start Airflow

```bash
echo "AIRFLOW_UID=50000" > .env
docker compose up -d
```

> **Airflow UI** → [http://localhost:8080](http://localhost:8080) — login: `airflow` / `airflow`

### 4️⃣ Run the Pipeline

```bash
# Terminal 1 — Start producing events
python scripts/producer.py

# Terminal 2 — Start consuming & writing to Delta Lake
python scripts/consumer.py
```

### 5️⃣ Reset Delta Lake (after schema changes)

```bash
# Windows (Admin PowerShell)
Remove-Item -Recurse -Force delta\checkpoints, delta\output

# Linux / macOS
rm -rf delta/checkpoints delta/output
```

---

## 🌬️ Airflow DAGs

### DAG 01 — Hello World

```mermaid
graph LR
    A["🖥️ check_environment<br/><i>BashOperator</i>"] --> B["📊 generate_pipeline_info<br/><i>PythonOperator</i>"]
    B --> C["🔗 consume_pipeline_info<br/><i>PythonOperator</i>"]
    C --> D["📋 pipeline_summary<br/><i>BashOperator + Jinja</i>"]

    style A fill:#2d3436,stroke:#00b894,color:#fff
    style B fill:#2d3436,stroke:#0984e3,color:#fff
    style C fill:#2d3436,stroke:#0984e3,color:#fff
    style D fill:#2d3436,stroke:#00b894,color:#fff
```

**Concepts:** `BashOperator` • `PythonOperator` • XCom push/pull • Jinja templating

---

### DAG 02 — E-commerce ETL Pipeline

```mermaid
graph LR
    E["📥 extract<br/><i>Generate 50 transactions</i>"] --> T["🔄 transform<br/><i>Pandas: TTC + flags</i>"]
    T --> Q["✅ quality_gate<br/><i>Volume · Nulls · Values</i>"]
    Q --> L["📊 load_summary<br/><i>Revenue · Rates · Metrics</i>"]
    L --> C["🧹 cleanup<br/><i>Remove temp files</i>"]

    style E fill:#2d3436,stroke:#6c5ce7,color:#fff
    style T fill:#2d3436,stroke:#0984e3,color:#fff
    style Q fill:#2d3436,stroke:#e17055,color:#fff
    style L fill:#2d3436,stroke:#00b894,color:#fff
    style C fill:#2d3436,stroke:#636e72,color:#fff
```

**Concepts:** ETL pattern • Quality gates • XCom data passing • Pandas transforms

---

### DAG 03 — Data Quality Branching

```mermaid
graph TD
    R["🔍 run_quality_checks"] --> D{"🔀 decide_branch"}
    D -->|"rows ≥ 20 AND<br/>anomaly < 20%"| W["📦 load_to_warehouse"]
    D -->|"otherwise"| A["🚨 trigger_alert"]
    W --> F["📋 final_report<br/><i>trigger: ALL_DONE</i>"]
    A --> F
    F --> E["🏁 end"]

    style R fill:#2d3436,stroke:#0984e3,color:#fff
    style D fill:#2d3436,stroke:#fdcb6e,color:#2d3436
    style W fill:#2d3436,stroke:#00b894,color:#fff
    style A fill:#2d3436,stroke:#e17055,color:#fff
    style F fill:#2d3436,stroke:#636e72,color:#fff
    style E fill:#2d3436,stroke:#636e72,color:#fff
```

**Concepts:** `BranchPythonOperator` • `TriggerRule.ALL_DONE` • Conditional routing • Alert simulation

---

## 📊 Progress

| Phase | Description | Status | Progress |
|:-----:|:------------|:------:|:--------:|
| **1** | Airflow DAGs (BashOp, PythonOp, XCom, Branch) | ✅ Done | `████████████` 100% |
| **2** | Kafka (Producer + Topic + Kafka UI) | ✅ Done | `████████████` 100% |
| **3** | Delta Lake (PySpark Streaming → Parquet) | ✅ Done | `███████████░` 95% |
| **4** | Full Pipeline (Unified Docker Compose) | 🔴 TODO | `░░░░░░░░░░░░` 0% |

---

## 📊 Transaction Schema

```json
{
  "id_transaction": "550e8400-e29b-41d4-a716-446655440000",
  "user_id": 42,
  "montant": 150.75,
  "methode_paiement": "Carte Bancaire",
  "statut": "SUCCESS",
  "event_timestamp": "2026-07-17T10:30:00.000000"
}
```

| Field | Type | Description |
|-------|------|-------------|
| `id_transaction` | `string (UUID)` | Unique transaction ID |
| `user_id` | `int [1-100]` | Customer ID — used as Kafka partition key |
| `montant` | `float [10-500]` | Transaction amount in EUR |
| `methode_paiement` | `string` | `Carte Bancaire` · `PayPal` · `Virement` |
| `statut` | `string` | `SUCCESS` (90%) · `FAILED` (10%) |
| `event_timestamp` | `string (ISO)` | UTC timestamp of the event |

---

## ⚙️ Environment Variables

Copy `.env.example` → `.env` and adjust:

| Variable | Default | Description |
|----------|---------|-------------|
| `HADOOP_HOME` | `./hadoop` | Path to Hadoop binaries |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | Kafka broker address |
| `KAFKA_TOPIC` | `transactions_ecommerce` | Topic name |
| `DELTA_OUTPUT_PATH` | `./delta/output` | Delta table directory |
| `DELTA_CHECKPOINT_PATH` | `./delta/checkpoints` | Streaming checkpoint dir |
| `SPARK_KAFKA_PACKAGE` | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` | Spark-Kafka connector |
| `DELTA_SPARK_PACKAGE` | `io.delta:delta-spark_2.12:3.1.0` | Delta-Spark connector |
| `AIRFLOW_UID` | `50000` | Linux UID for Airflow containers |

---

## 👤 Author

**Abdelhakim Mahdad**

<p>
  <a href="https://github.com/MdAbdehakim"><img src="https://img.shields.io/badge/GitHub-MdAbdehakim-181717?style=flat-square&logo=github" alt="GitHub"/></a>
</p>

---

<p align="center">
  <sub>Built with ☕ and determination — Month 1 of the Big Data Engineer journey</sub>
</p>
