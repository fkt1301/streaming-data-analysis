"""
Spark Structured Streaming job: Kafka -> Postgres.

Open-source Spark has no native Confluent Schema Registry integration,
so we do it manually:
  1. Fetch the current schema from the registry at startup.
  2. Each Kafka message's value is wire-encoded as:
       [1 magic byte][4-byte schema ID][avro payload]
     We strip the first 5 bytes before handing the rest to from_avro.

Run with:
  uv run spark-submit \
    --packages org.apache.spark:spark-avro_2.12:3.5.1 \
    -m consumer.main
(see note below on why spark-submit, not plain python, is needed)
"""
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr

from consumer.config import ConsumerSettings
from consumer.schema import fetch_latest_schema

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("consumer")


def write_batch_to_postgres(settings: ConsumerSettings):
    """Returns a foreachBatch function bound to these settings."""

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        count = batch_df.count()
        if count == 0:
            return
        log.info("Writing batch %s with %s rows to Postgres", batch_id, count)
        (
            batch_df.write.jdbc(
                url=settings.jdbc_url,
                table="created_users",
                mode="append",
                properties={
                    "user": settings.postgres_user,
                    "password": settings.postgres_password,
                    "driver": "org.postgresql.Driver",
                },
            )
        )

    return _write


def main() -> None:
    settings = ConsumerSettings()
    log.info("Fetching schema from registry: %s", settings.schema_registry_url)
    schema_str = fetch_latest_schema(settings.schema_registry_url, f"{settings.kafka_topic}-value")

    spark = (
        SparkSession.builder.appName("KafkaToPostgresStream")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.apache.spark:spark-avro_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.3",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Strip the 5-byte Confluent wire-format header (1 magic byte + 4-byte
    # schema ID), then decode the remaining Avro payload.
    stripped = raw_stream.withColumn(
        "avro_payload", expr("substring(value, 6, length(value) - 5)")
    )
    parsed = stripped.select(from_avro(col("avro_payload"), schema_str).alias("data")).select(
        "data.*"
    )

    query = (
        parsed.writeStream.foreachBatch(write_batch_to_postgres(settings))
        .option("checkpointLocation", "/tmp/spark-checkpoints/users_created")
        .trigger(processingTime="5 seconds")
        .start()
    )

    log.info("Streaming query started, awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
