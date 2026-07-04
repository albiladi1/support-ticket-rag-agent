"""
Deliverable 2: Delta Lakehouse — bronze/silver/gold zones with MERGE and
schema enforcement.

Bronze : raw validated tickets, exactly as they arrived from Kafka.
Silver : cleaned/deduplicated tickets (text normalized, nulls handled).
Gold   : analytics-ready tickets, one row per ticket_id, ready for chunking
         + embedding in the RAG pipeline. Re-ingesting an updated ticket
         (same ticket_id) triggers a real MERGE (upsert), not a full
         overwrite.

Run: python delta_lakehouse.py
"""
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType
from dotenv import load_dotenv

load_dotenv()

BRONZE_LANDING_FILE = os.path.join(
    os.path.dirname(__file__), "..", "lakehouse", "bronze", "_landing", "tickets.jsonl"
)
BRONZE_PATH = os.getenv("BRONZE_PATH", "./lakehouse/bronze/tickets")
SILVER_PATH = os.getenv("SILVER_PATH", "./lakehouse/silver/tickets")
GOLD_PATH = os.getenv("GOLD_PATH", "./lakehouse/gold/tickets")

TICKET_SPARK_SCHEMA = StructType([
    StructField("ticket_id", StringType(), nullable=False),
    StructField("customer", StringType(), nullable=False),
    StructField("subject", StringType(), nullable=False),
    StructField("body", StringType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("created_at", StringType(), nullable=False),
])


def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("ticket-lakehouse")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def write_bronze(spark: SparkSession):
    """Append-only landing of validated raw events. Schema is enforced by
    passing an explicit StructType — any drift from Kafka raises immediately
    instead of silently corrupting downstream zones."""
    df = spark.read.schema(TICKET_SPARK_SCHEMA).json(BRONZE_LANDING_FILE)
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "false")  # strict schema enforcement
        .save(BRONZE_PATH)
    )
    print(f"[bronze] wrote {df.count()} rows -> {BRONZE_PATH}")
    return df


def upsert_silver(spark: SparkSession, bronze_df):
    """Clean + deduplicate, then MERGE (upsert) into the Silver Delta table
    on ticket_id — reprocessing an updated ticket updates the existing row
    instead of duplicating it."""
    from delta.tables import DeltaTable

    silver_df = (
        bronze_df.withColumn("subject", F.trim(F.col("subject")))
        .withColumn("body", F.trim(F.col("body")))
        .withColumn("category", F.lower(F.col("category")))
        .dropDuplicates(["ticket_id"])
    )

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        (
            silver_table.alias("target")
            .merge(silver_df.alias("source"), "target.ticket_id = source.ticket_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("[silver] MERGE complete (upserted changed/new tickets)")
    else:
        silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
        print(f"[silver] initial write -> {SILVER_PATH}")

    return spark.read.format("delta").load(SILVER_PATH)


def upsert_gold(spark: SparkSession, silver_df):
    """Gold = one enriched, RAG-ready row per ticket: a single 'document'
    text field (subject + body) that the RAG pipeline will chunk/embed."""
    from delta.tables import DeltaTable

    gold_df = silver_df.withColumn(
        "document_text", F.concat_ws(". ", F.col("subject"), F.col("body"))
    ).select("ticket_id", "customer", "category", "document_text", "created_at")

    if DeltaTable.isDeltaTable(spark, GOLD_PATH):
        gold_table = DeltaTable.forPath(spark, GOLD_PATH)
        (
            gold_table.alias("target")
            .merge(gold_df.alias("source"), "target.ticket_id = source.ticket_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        print("[gold] MERGE complete")
    else:
        gold_df.write.format("delta").mode("overwrite").save(GOLD_PATH)
        print(f"[gold] initial write -> {GOLD_PATH}")

    return spark.read.format("delta").load(GOLD_PATH)


def run_pipeline():
    spark = get_spark()
    bronze_df = write_bronze(spark)
    silver_df = upsert_silver(spark, bronze_df)
    gold_df = upsert_gold(spark, silver_df)
    gold_df.show(truncate=80)
    spark.stop()


if __name__ == "__main__":
    run_pipeline()