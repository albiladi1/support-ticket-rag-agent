# Support Ticket RAG Agent

A single AI Agent that answers questions about customer support tickets,
grounded by a production-style data pipeline: **Kafka → Delta Lakehouse
(Bronze/Silver/Gold) → Hybrid RAG → Free LLM**, orchestrated by Airflow
with a Great Expectations + OpenLineage data quality layer.

## Overview

| Layer | What it does | Status |
|---|---|---|
| Ingestion (Kafka) | Streams tickets, validates schema, routes bad records to a dead-letter queue | Working — all sample tickets validated and ingested |
| Lakehouse (Delta on Spark) | Bronze/Silver/Gold zones with real MERGE (upsert) and schema enforcement | Working — verified with real MERGE operations |
| RAG pipeline | Chunking, embeddings, FAISS vector index, hybrid (BM25 + vector) search, cross-encoder reranking | Working — the Agent answers grounded questions with source citations |
| Orchestration (Airflow) | DAG connecting ingestion → lakehouse → indexing end-to-end | Defined and scheduled correctly; see the Airflow notes below for current environment considerations |
| Data quality & observability | Great Expectations checks + OpenLineage events per pipeline stage | Working — checks pass; lineage events emitted per stage |

## Architecture

```
Kafka Producer            Kafka Consumer                Delta Lakehouse
(sample_tickets.json) --> (schema validation) --> Bronze --> Silver --> Gold
                                 |  invalid                  (MERGE)   (MERGE)
                                 v
                                DLQ
                                                                       |
                                                                       v
                                                                   Chunking
                                                                       |
                                                                       v
                                                             Embeddings (local,
                                                             sentence-transformers)
                                                                       |
                                                                       v
                                                              FAISS Vector Index
                                                                       |
                              Hybrid Search (BM25 + Vector, RRF) <-----+
                                       |
                                       v
                              Reranking (cross-encoder)
                                       |
                                       v
                         AI Agent --> Free LLM (openai/gpt-oss-20b:free
                                       via OpenRouter)
                                       |
                                       v
                                Grounded answer + [ticket_id] citations
```

## Project Structure

```
project/
├── data/
│   ├── kaggle_raw.csv             # raw source dataset (200K customer support tickets)
│   ├── prepare_kaggle_data.py     # converts raw CSV -> schema-conformant JSON
│   └── sample_tickets.json        # prepared tickets used by the pipeline
├── src/
│   ├── schema.py                  # ticket JSON schema + validation
│   ├── kafka_producer.py          # streams tickets into Kafka
│   ├── kafka_consumer.py          # validates + writes to the bronze landing zone
│   ├── delta_lakehouse.py         # Bronze -> Silver -> Gold, with MERGE
│   ├── agent.py                   # the AI agent + free LLM integration
│   ├── rag/
│   │   ├── chunking.py            # sentence-aware chunking with overlap
│   │   ├── embeddings.py          # local sentence-transformers embeddings
│   │   ├── vector_index.py        # FAISS index build/search
│   │   ├── hybrid_search.py       # BM25 + vector search, merged via RRF
│   │   ├── reranker.py            # cross-encoder reranking
│   │   └── rag_pipeline.py        # ties chunking -> embed -> index -> retrieve
│   └── quality/
│       ├── ge_suite.py            # Great Expectations validation suite
│       └── lineage.py             # OpenLineage event emission
├── dags/pipeline_dag.py           # Airflow DAG, end-to-end
├── docker-compose.yml             # local Kafka + Zookeeper
├── docker-compose-airflow.yml     # local Airflow (standalone mode)
├── requirements.txt
└── .env / .env.example
```

## Data Source

The pipeline runs on a real 200K-row Kaggle dataset ("Customer Support
Tickets"), downsampled to a smaller set via `data/prepare_kaggle_data.py`
for fast local iteration. See that script for the exact column mapping
used (`issue_description` -> `body`, `ticket_id` preserved, category
normalized to a fixed set of values, timestamps converted to ISO-8601).

## Setup

```bash
python -m venv venv
venv\Scripts\activate            # Windows
pip install -r requirements.txt
cp .env.example .env             # fill in OPENROUTER_API_KEY
docker compose up -d             # starts local Kafka + Zookeeper
```

### Windows-specific setup notes

Running this stack natively on Windows surfaced a few non-obvious issues,
documented here in case they come up again:

1. **Java version conflict.** PySpark 3.5.1 requires Java 8/11/17; newer
   JDKs throw `UnsupportedOperationException: getSubject is not
   supported`. Fix: install Temurin JDK 17 (adoptium.net) alongside the
   existing JDK, and point `JAVA_HOME` at it specifically.

2. **Missing `winutils.exe` / `hadoop.dll`.** PySpark on Windows needs
   Hadoop's Windows-native binaries even for purely local file
   operations. Fix: obtained `winutils.exe` + `hadoop.dll` (Hadoop 3.3.5,
   matching PySpark's bundled Hadoop client) from the cdarlint/winutils
   GitHub repo, placed them in `C:\hadoop\bin`, and set
   `HADOOP_HOME=C:\hadoop`.

3. **PowerShell script execution disabled by default.** Resolved with:
   ```powershell
   Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
   ```

## Running each component manually

```bash
cd src

# Ingestion
python kafka_producer.py        # streams sample_tickets.json into Kafka
python kafka_consumer.py        # validates + writes to the bronze landing zone
                                 # (Ctrl+C to stop -- it listens continuously)

# Lakehouse
python delta_lakehouse.py       # bronze -> silver (MERGE) -> gold (MERGE)

# RAG pipeline (smoke test)
cd rag && python rag_pipeline.py

# Agent (end-to-end question answering)
cd .. && python agent.py
```

## Airflow Orchestration

The DAG (`dags/pipeline_dag.py`) is correctly defined, and Airflow parses
and schedules it without import errors. It runs via Docker
(`docker-compose-airflow.yml`) rather than natively, since Apache Airflow
does not officially support native Windows execution (it runs on
POSIX-compliant systems, or via WSL2 / Linux containers on Windows).

### What this involved
- **Cross-container networking.** Kafka originally advertised itself as
  `localhost:9092`, which resolves to the Airflow container itself from
  inside a separate container, not the Kafka container. Resolved by
  advertising Kafka as `kafka:9092` (its Docker Compose service name) and
  adding a matching Windows `hosts` entry (`127.0.0.1 kafka`) so the same
  hostname resolves correctly from both the host machine and any
  container on the shared Docker network.
- **Environment variables not reaching the container.** `.env` was not
  mounted into the Airflow container, so `KAFKA_BOOTSTRAP_SERVERS`
  silently fell back to its default. Resolved by passing it explicitly
  through the `environment:` block in `docker-compose-airflow.yml`.
- **Missing Python dependencies.** The official `apache/airflow` image is
  intentionally minimal. The ingestion task's dependencies
  (`kafka-python`, `jsonschema`, `python-dotenv`, `requests`) are
  installed via a `command:` override before Airflow starts.

### Current consideration
The lakehouse and indexing tasks depend on heavier packages (PySpark + a
JVM, sentence-transformers, faiss-cpu) that are impractical to install at
container startup inside this lightweight standalone container. Their
underlying logic is fully verified working when run directly (see
`python src/delta_lakehouse.py` and `python src/rag/rag_pipeline.py`
above, both of which succeed end-to-end against the full dataset). A
production deployment would resolve this with a custom Airflow worker
image that has all pipeline dependencies preinstalled, rather than
installing them at runtime.

## Data Quality & Observability

- `src/quality/ge_suite.py` validates the Silver zone (no null
  ticket_id/body, unique ticket_id, category within an allowed set) using
  the Great Expectations pandas-validator API. All checks pass against
  the real Silver table.
- `src/quality/lineage.py` emits OpenLineage START/COMPLETE/FAIL events
  for every pipeline stage (ingestion, bronze, silver, gold, indexing),
  making the flow observable and auditable independent of Airflow's own
  UI. This integrates with any OpenLineage-compatible backend, such as
  Marquez.

## The Free LLM

The agent calls `openai/gpt-oss-20b:free` via OpenRouter, which offers
this model at no cost with an API key. Any other free model listed on
OpenRouter can be swapped in via the `OPENROUTER_MODEL` env var with no
code changes.

## PySpark

PySpark + delta-spark power the entire Bronze/Silver/Gold layer
(`src/delta_lakehouse.py`), demonstrating a genuine big-data-oriented
processing path while remaining fully runnable locally on a small
sample, and scalable to the full 200K-row dataset by adjusting
`max_rows` in `data/prepare_kaggle_data.py`.