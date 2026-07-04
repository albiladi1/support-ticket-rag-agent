"""
Deliverable 4: Airflow DAG connecting all modules end-to-end.

Flow:
  consume_kafka -> bronze -> silver (GE quality gate) -> gold -> vector_index

Each task is wrapped with an OpenLineage lineage stage for observability.
Schedule: hourly, simulating a periodic refresh of the RAG knowledge base.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

SRC_DIR = os.path.join(os.path.dirname(__file__), "..", "src")
sys.path.append(SRC_DIR)
sys.path.append(os.path.join(SRC_DIR, "rag"))
sys.path.append(os.path.join(SRC_DIR, "quality"))

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def task_consume_kafka(**_):
    from kafka_consumer import consume_and_validate
    from lineage import LineageStage

    with LineageStage("kafka_ingest"):
        consume_and_validate(max_messages=100)


def task_bronze_silver_gold(**_):
    from delta_lakehouse import get_spark, write_bronze, upsert_silver, upsert_gold
    from lineage import LineageStage
    from ge_suite import validate_silver

    spark = get_spark()
    with LineageStage("bronze_ingest"):
        bronze_df = write_bronze(spark)

    with LineageStage("silver_merge"):
        silver_df = upsert_silver(spark, bronze_df)
        validate_silver(silver_df.toPandas())  # quality gate: raises on failure

    with LineageStage("gold_merge"):
        gold_df = upsert_gold(spark, silver_df)
        gold_df.write.mode("overwrite").json(
            os.path.join(os.path.dirname(__file__), "..", "lakehouse", "_gold_export")
        )

    spark.stop()


def task_build_vector_index(**_):
    import json

    from rag_pipeline import index_gold_zone
    from lineage import LineageStage

    export_dir = os.path.join(os.path.dirname(__file__), "..", "lakehouse", "_gold_export")
    rows = []
    for fname in os.listdir(export_dir):
        if fname.endswith(".json"):
            with open(os.path.join(export_dir, fname)) as f:
                rows.extend(json.loads(line) for line in f if line.strip())

    with LineageStage("vector_index_build"):
        index_gold_zone(rows)


with DAG(
    dag_id="support_ticket_rag_pipeline",
    description="Kafka -> Delta Lakehouse (Bronze/Silver/Gold) -> RAG vector index",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["rag", "lakehouse", "capstone"],
) as dag:

    ingest = PythonOperator(task_id="consume_kafka", python_callable=task_consume_kafka)
    lakehouse = PythonOperator(task_id="bronze_silver_gold", python_callable=task_bronze_silver_gold)
    index = PythonOperator(task_id="build_vector_index", python_callable=task_build_vector_index)

    ingest >> lakehouse >> index