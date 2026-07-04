## Data Source

The pipeline runs on a real 200K-row Kaggle dataset ("Customer Support
Tickets"), downsampled to 2000 rows via `data/prepare_kaggle_data.py` for
fast local iteration. See that script for the exact column mapping used
(`issue_description` -> `body`, `ticket_id` preserved, category
normalized to the 4 allowed values, timestamps converted to ISO-8601).

## Setup

```bash
python -m venv venv
venv\Scripts\activate            # Windows
pip install -r requirements.txt
cp .env.example .env             # fill in OPENROUTER_API_KEY
docker compose up -d             # starts local Kafka + Zookeeper
```

### Windows-specific setup notes (things that tripped us up)

Running this stack natively on Windows required three non-obvious fixes,
documented here in case you hit the same issues:

1. **Java version conflict.** PySpark 3.5.1 requires Java 8/11/17; Java 24
   throws `UnsupportedOperationException: getSubject is not supported`.
   Fix: install Temurin JDK 17 (adoptium.net) alongside the existing JDK,
   and point `JAVA_HOME` at it specifically.

2. **Missing `winutils.exe` / `hadoop.dll`.** PySpark on Windows needs
   Hadoop's Windows-native binaries even for purely local file operations.
   Fix: downloaded `winutils.exe` + `hadoop.dll` (Hadoop 3.3.5, matching
   PySpark's bundled `hadoop-client-api-3.3.4`) from the cdarlint/winutils
   GitHub repo, placed them in `C:\hadoop\bin`, and set
   `HADOOP_HOME=C:\hadoop`.

3. **PowerShell script execution disabled by default.** Fixed with:
```powershell
   Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned
```

## Running each deliverable manually (for demoing/grading)

```bash
cd src

# 1) Ingestion layer
python kafka_producer.py        # streams sample_tickets.json into Kafka
python kafka_consumer.py        # validates + writes to bronze landing zone
                                 # (Ctrl+C to stop -- it listens continuously)

# 2) Delta Lakehouse
python delta_lakehouse.py       # bronze -> silver (MERGE) -> gold (MERGE)

# 3) RAG pipeline (smoke test)
cd rag && python rag_pipeline.py

# Agent (end-to-end question answering)
cd .. && python agent.py
```

## Airflow Orchestration -- Current Status

The DAG (`dags/pipeline_dag.py`) is correctly defined and Airflow parses
and schedules it without import errors. It was deployed via Docker
(`docker-compose-airflow.yml`) rather than natively, since Apache Airflow
does not officially support native Windows execution (it prints this
warning on every startup: "Airflow currently can be run on
POSIX-compliant Operating Systems... via WSL2 or Linux Containers").

### What was fixed
- **Cross-container networking**: Kafka originally advertised itself as
  `localhost:9092`, which resolves to the Airflow container itself from
  inside a separate container, not the Kafka container. Fixed by changing
  Kafka's `KAFKA_ADVERTISED_LISTENERS` to `kafka:9092` (the Docker Compose
  service name) and adding a matching Windows `hosts` file entry
  (`127.0.0.1 kafka`) so the same hostname resolves correctly from both
  the host machine and any container on the same Docker network.
- **Missing environment variables in the container**: `.env` was never
  mounted into the Airflow container, so `KAFKA_BOOTSTRAP_SERVERS`
  silently fell back to its `localhost:9092` default. Fixed by passing it
  explicitly via the `environment:` block in `docker-compose-airflow.yml`.
- **Missing Python dependencies**: the official `apache/airflow` image is
  intentionally minimal and does not include `kafka-python` or any of
  this project's other dependencies. The `consume_kafka` task's
  dependencies (`kafka-python`, `jsonschema`, `python-dotenv`, `requests`)
  are now installed via a `command:` override before Airflow starts.

### Known remaining limitation
The `bronze_silver_gold` and `build_vector_index` tasks require heavy
dependencies (PySpark + a JVM, sentence-transformers, faiss-cpu) that are
impractical to install at container startup (would require replicating
the Java/winutils setup documented above inside the container, plus
several hundred MB of downloads on every restart). These two tasks are
expected to fail inside this lightweight standalone container -- their
underlying logic is fully verified working when run directly (see
`python src/delta_lakehouse.py` and `python src/rag/rag_pipeline.py`
above, both of which succeed end-to-end against the real 2000-row
dataset). In a production deployment, this is solved with a custom
Airflow worker image built with all pipeline dependencies preinstalled (a
Dockerfile extending `apache/airflow:2.9.3` with a requirements.txt
layer) rather than installing them at runtime.

## Data Quality & Lineage (Deliverable 5)

- `src/quality/ge_suite.py` validates the Silver zone (no null
  ticket_id/body, unique ticket_id, category in an allowed set) using the
  `great_expectations==0.18.19` pandas-validator API. Verified: 4/4
  expectations pass against the real 2000-row Silver table.
- `src/quality/lineage.py` emits OpenLineage START/COMPLETE/FAIL events
  for every stage (`kafka_ingest`, `bronze_ingest`, `silver_merge`,
  `gold_merge`, `vector_index_build`). Verified: the client correctly
  attempts delivery to `http://localhost:5000` (fails with
  ConnectionRefusedError only because no OpenLineage backend such as
  Marquez is running locally -- the emission logic itself is confirmed
  correct).

## Notes on the free LLM

The agent calls `openai/gpt-oss-20b:free` via OpenRouter, which offers
this model at no cost with an API key. Any other free model listed on
OpenRouter can be swapped in via the `OPENROUTER_MODEL` env var with no
code changes.

## PySpark (bonus, not mandatory)

PySpark + delta-spark power the entire Bronze/Silver/Gold layer
(`src/delta_lakehouse.py`), satisfying the "dealing with big data" bonus
criterion while remaining fully runnable locally on the 2000-row sample
(and scalable to the full 200K-row dataset by adjusting `max_rows` in
`data/prepare_kaggle_data.py`).