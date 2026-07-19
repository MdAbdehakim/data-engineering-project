import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# ── Configuration via environment variables ──────────────────────────────────
PROJECT_DIR = Path(__file__).resolve().parent.parent

HADOOP_HOME = os.environ.get("HADOOP_HOME", "C:/hadoop")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions_ecommerce")
DELTA_OUTPUT_PATH = os.environ.get("DELTA_OUTPUT_PATH", str(PROJECT_DIR / "delta" / "output"))
DELTA_CHECKPOINT_PATH = os.environ.get("DELTA_CHECKPOINT_PATH", str(PROJECT_DIR / "delta" / "checkpoints"))

SPARK_KAFKA_PACKAGE = os.environ.get(
    "SPARK_KAFKA_PACKAGE",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
)
DELTA_SPARK_PACKAGE = os.environ.get(
    "DELTA_SPARK_PACKAGE",
    "io.delta:delta-spark_2.12:3.1.0",
)


def process_stream():

    os.environ["HADOOP_HOME"] = HADOOP_HOME          # ← ajouter
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable  
    spark = (
        SparkSession.builder
        .appName("KafkaToDeltaLake")
        .config("spark.jars.packages", f"{SPARK_KAFKA_PACKAGE},{DELTA_SPARK_PACKAGE}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("id_transaction", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("montant", DoubleType(), True),
        StructField("methode_paiement", StringType(), True),
        StructField("statut", StringType(), True),
        StructField("event_timestamp", StringType(), True),
    ])

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )

    parsed_df = parsed_df.withColumn("processing_timestamp", current_timestamp())

    query = None
    try:
        query = (
            parsed_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", DELTA_CHECKPOINT_PATH)
            .start(DELTA_OUTPUT_PATH)
        )

        print("🚀 Pipeline PySpark démarré... En attente des données de Kafka.")
        query.awaitTermination()
    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    process_stream()