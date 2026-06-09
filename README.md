# Market Pulse 📈

A production-grade real-time financial data platform built to demonstrate 
modern data engineering practices across the full stack. Processes live 
stock market data through a complete pipeline from ingestion to serving, 
incorporating streaming, batch, AI, and governance layers.

---

## Architecture
┌─────────────────────────────────────┐
                │           INGESTION LAYER            │
                │                                      │
                │  Yahoo Finance / Synthetic Producer  │
                │         (confluent-kafka)            │
                └──────────────┬──────────────────────┘
                               │
                               ▼
                ┌─────────────────────────────────────┐
                │           MESSAGE LAYER              │
                │                                      │
                │      Apache Kafka (KRaft mode)       │
                │    Topic: stock.price.ticks          │
                │    Partitioned by symbol             │
                └──────┬────────────────┬─────────────┘
                       │                │
           ┌───────────▼───┐    ┌───────▼──────────────┐
           │  BATCH LAYER  │    │   STREAMING LAYER     │
           │               │    │                       │
           │    Spark      │    │   Apache Flink        │
           │  Structured   │    │   OHLCV Windows       │
           │  Streaming    │    │   Price Alerts        │
           └───────┬───────┘    └───────────────────────┘
                   │
                   ▼
    ┌──────────────────────────────────┐
    │         STORAGE LAYER            │
    │                                  │
    │  Delta Lake (Medallion)          │
    │  ├── Bronze: raw ticks           │
    │  ├── Silver: cleaned (dbt)       │
    │  └── Gold: aggregated (dbt)      │
    │                                  │
    │  Snowflake (warehouse)           │
    │  ClickHouse (OLAP)               │
    └──────────────┬───────────────────┘
                   │
                   ▼
    ┌──────────────────────────────────┐
    │         SERVING LAYER            │
    │                                  │
    │  Redis (cache + pub/sub)         │
    │  FastAPI (REST + WebSocket)      │
    │  Streamlit (dashboard)           │
    └──────────────────────────────────┘
                   │
                   ▼
    ┌──────────────────────────────────┐
    │           AI LAYER               │
    │                                  │
    │  RAG Pipeline (Pinecone)         │
    │  LangChain + Groq LLM            │
    │  LLMOps (prompt tracking)        │
    └──────────────────────────────────┘

## Data Flow
Producer → Kafka → Spark Structured Streaming → Delta Bronze
│
Great Expectations
Quality Gate (20 checks)
│
dbt Silver transforms
│
dbt Gold aggregations
│
Snowflake ← → ClickHouse
│
FastAPI + Redis
│
Streamlit Dashboard
│
RAG + LLM Q&AProducer → Kafka → Flink OHLCV Windows → Price Alerts → Redis pub/sub
Producer → Kafka → Flink OHLCV Windows → Price Alerts → Redis pub/sub

---

## Technology Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| Ingestion | Python, confluent-kafka | Stock tick producer |
| Message bus | Apache Kafka 7.7 (KRaft) | Event streaming |
| Batch streaming | Apache Spark 3.5 | Bronze layer ingestion |
| Real-time streaming | Apache Flink 1.18 | OHLCV alerts |
| Storage format | Delta Lake + Parquet | ACID data lake |
| Warehouse | Snowflake | Governed analytics |
| OLAP | ClickHouse | Sub-second queries |
| Transformation | dbt | Silver + Gold models |
| Orchestration | Apache Airflow | Pipeline scheduling |
| Quality | Great Expectations | 20 automated checks |
| Cache | Redis | Live price + pub/sub |
| API | FastAPI | REST + WebSocket |
| Dashboard | Streamlit | Live visualization |
| AI | LangChain + Pinecone + Groq | RAG pipeline |
| Infrastructure | Docker, GitHub Actions | IaC + CI/CD |

---

## Local Setup

### Prerequisites
- Docker Desktop (running)
- Python 3.11+
- Git
- Java 17 (for PyFlink)

### Installation

```bash
# Clone repository
git clone https://github.com/atulpandey02/market-pulse.git
cd market-pulse

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Copy environment variables
cp .env.example .env
```

### Start Infrastructure

```bash
cd infra
docker compose up -d
docker compose ps
```

Services started:
- Kafka (KRaft) → localhost:9092
- Kafka UI → localhost:8080
- Spark Master → localhost:9090
- Spark Worker → localhost:9091
- Flink JobManager → localhost:8081
- Redis → localhost:6379
- ClickHouse → localhost:8123

### Run the Pipeline

```bash
# Start producer (continuous mode)
cd ingestion
python producer.py

# Start Spark Bronze stream (new terminal)
docker cp streaming/bronze_stream.py spark-master:/opt/spark/work-dir/
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
io.delta:delta-spark_2.12:3.0.0 \
  --conf spark.sql.extensions=\
io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=\
org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /opt/spark/work-dir/bronze_stream.py

# Run Flink price alerts (new terminal)
export JAVA_HOME=/opt/homebrew/Cellar/openjdk@17/17.0.16/\
libexec/openjdk.jdk/Contents/Home
python streaming/flink_price_alerts.py

# Run quality checks (new terminal)
python observability/bronze_quality_checks.py
```

---

## Project Structure
market-pulse/
├── ingestion/                  # Kafka producers and consumers
│   ├── producer.py             # Stock price tick producer (OOP)
│   └── consumer.py             # Manual offset commit consumer
│
├── streaming/                  # Stream processing jobs
│   ├── bronze_stream.py        # Spark → Delta Lake Bronze
│   ├── read_bronze.py          # Delta Lake reader utility
│   └── flink_price_alerts.py  # Flink OHLCV + alerts
│
├── warehouse/                  # Data warehouse layer
│   └── (dbt models — Week 2)
│
├── serving/                    # API and cache layer
│   └── (FastAPI + Redis — Week 3)
│
├── ai/                         # AI and ML layer
│   └── (RAG pipeline — Week 4)
│
├── observability/              # Data quality and monitoring
│   ├── bronze_quality_checks.py  # Great Expectations gate
│   └── great_expectations/       # GE configuration
│
├── infra/                      # Infrastructure as code
│   └── docker-compose.yml      # All services
│
├── tests/                      # Unit tests
│   └── test_tick_generator.py  # 9 tests, all passing
│
├── .github/
│   └── workflows/
│       └── ci.yml              # Lint + test on every push
│
├── requirements.txt            # Full dev dependencies
├── requirements-ci.txt         # Lightweight CI dependencies
├── .env.example                # Environment variable template
└── README.md                   # This file

---

## Production Principles Applied

| Principle | Implementation |
|-----------|---------------|
| **Idempotency** | Microsecond event_id + consumer deduplication |
| **Immutability** | Bronze layer append-only, never modified |
| **Exactly-once** | Flink checkpointing + Kafka offset management |
| **Quality gates** | Great Expectations blocks bad data before Silver |
| **Data lineage** | kafka_partition + kafka_offset stored in Bronze |
| **Schema enforcement** | Explicit schemas in Spark + GE type checks |
| **Separation of concerns** | OOP with SRP — one class, one responsibility |
| **Dependency injection** | Dedup store injected — swappable local to production |
| **IaC** | Docker Compose + GitHub Actions from day one |
| **Observability** | source_timestamp vs ingestion_timestamp tracked |
| **Graceful shutdown** | Signal handlers on all long-running processes |
| **Exit codes** | sys.exit(1) on failure — Airflow-compatible |

---

## Key Learnings

### Kafka
- KRaft mode removes Zookeeper dependency (Kafka 3.3+)
- Two listeners needed: localhost for Mac, internal for Docker
- Replication factor cannot exceed broker count
- Offset commit after processing = at-least-once guarantee

### Spark
- Lazy evaluation — plan before execute
- Shared volumes required for distributed Delta Lake access
- Micro-batch trigger interval controls latency vs file size
- source_timestamp vs ingestion_timestamp = pipeline latency

### Flink
- True streaming vs micro-batch — milliseconds vs seconds
- Event time vs processing time — always use event time for financial data
- Watermarks = bounded out-of-orderness tolerance
- Checkpointing = exactly-once via Chandy-Lamport algorithm
- AggregateFunction: create_accumulator → add → get_result → merge

### Data Quality
- Stop pipeline on critical failures (bad data worse than no data)
- Quarantine on non-critical failures (partial data acceptable)
- sys.exit(1) makes quality gate Airflow-compatible
- Synthetic test data for developing quality checks independently

---

## Roadmap

- [x] Week 1 — Kafka + Spark + Flink + Delta Lake + Quality
- [ ] Week 2 — Snowflake + ClickHouse + dbt + Airflow
- [ ] Week 3 — Redis + FastAPI + Streamlit dashboard
- [ ] Week 4 — RAG pipeline + LLMOps + AI observability

---

*Built over 30 days as a hands-on learning project.
Every design decision documented with production principles.*