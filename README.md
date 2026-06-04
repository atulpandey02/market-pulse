# Market Pulse 📈

A production-grade real-time financial data platform built for learning and demonstrating modern data engineering practices.

## Architecture
- **Ingestion**: Apache Kafka (price ticks, news, social sentiment)
- **Streaming**: Apache Spark Structured Streaming + Apache Flink
- **Storage**: S3 + Delta Lake (Bronze/Silver/Gold Medallion)
- **Warehouse**: Snowflake + ClickHouse (OLAP)
- **Orchestration**: Apache Airflow + dbt
- **Serving**: FastAPI + Redis + Streamlit
- **AI Layer**: RAG Pipeline (Pinecone + LangChain) + LLMOps
- **Observability**: Great Expectations + dbt lineage + SLA alerts

## Stack
`Python` `Kafka` `Spark` `Flink` `Snowflake` `ClickHouse` `Redis` `Databricks` `dbt` `Airflow` `Pinecone` `LangChain` `Docker` `GitHub Actions`

## Local Setup
```bash
cp .env.example .env
docker compose up -d
```

## Project Structure
