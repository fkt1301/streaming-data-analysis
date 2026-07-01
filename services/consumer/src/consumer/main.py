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
import json

import logging

from confluent_kafka import Producer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr, base64


from consumer.config import ConsumerSettings
from consumer.schema import fetch_latest_schema
from consumer.quality import split_valid_invalid


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("consumer")

def make_dlq_producer(settings: ConsumerSettings) -> Producer:
    return Producer({"bootstrap.servers": settings.kafka_bootstrap_servers})

def write_batch_to_postgres(settings: ConsumerSettings):
    dlq_producer = make_dlq_producer(settings)

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.count() == 0:
            return

        # `data` is null when from_avro couldn't parse the bytes at all.
        deser_failed_count = batch_df.filter(col("data").isNull()).count()
        if deser_failed_count > 0:
            log.error(
                "Batch %s: %s records failed Avro deserialization -- "
                "unexpected on a schema-governed topic, investigate upstream",
                batch_id,
                deser_failed_count,
            )

        # Flatten successfully decoded records and apply data-quality rules.
        decoded_df = batch_df.filter(col("data").isNotNull()).select("data.*")
        if decoded_df.count() == 0:
            return

        valid_df, invalid_df = split_valid_invalid(decoded_df)

        valid_count = valid_df.count()
        if valid_count > 0:
            log.info("Batch %s: writing %s valid rows to Postgres", batch_id, valid_count)
            valid_df.write.jdbc(
                url=settings.jdbc_url,
                table="created_users",
                mode="append",
                properties={
                    "user": settings.postgres_user,
                    "password": settings.postgres_password,
                    "driver": "org.postgresql.Driver",
                },
            )

        invalid_rows = invalid_df.collect()
        if invalid_rows:
            log.warning("Batch %s: routing %s invalid rows to DLQ", batch_id, len(invalid_rows))
            for row in invalid_rows:
                record = row.asDict()
                errors = record.pop("validation_errors")
                payload = {
                    "original_record": {k: str(v) for k, v in record.items()},
                    "validation_errors": list(errors),
                    "batch_id": batch_id,
                }
                dlq_producer.produce(
                    settings.dlq_topic,
                    key=str(record.get("id", "unknown")),
                    value=json.dumps(payload),
                )
            dlq_producer.flush(10)

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

    # Confluent wire format prepends 5 bytes before the Avro payload:
    # 1 magic byte (0x00) + 4-byte schema ID. Strip them before decoding.
    stripped = raw_stream.withColumn(
        "avro_payload", expr("substring(value, 6, length(value) - 5)")
    )
    decoded = stripped.withColumn("data", from_avro(col("avro_payload"), schema_str, {"mode": "PERMISSIVE"}))

    query = (
        decoded.writeStream.foreachBatch(write_batch_to_postgres(settings))
        .option("checkpointLocation", "/tmp/spark-checkpoints/users_created")
        .trigger(processingTime="5 seconds")
        .start()
    )

    log.info("Streaming query started, awaiting termination...")
    query.awaitTermination()


if __name__ == "__main__":
    main()
