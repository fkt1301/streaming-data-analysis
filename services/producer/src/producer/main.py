"""
Continuously streams randomuser.me records into Kafka, Avro-serialized
and schema-validated against the Schema Registry.

Run with: uv run python -m producer.main
"""
import logging
import sys
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import requests
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from producer.config import ProducerSettings
from producer.schema import load_schema_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("producer")


def fetch_raw_user(api_url: str) -> dict[str, Any]:
    response = requests.get(api_url, timeout=10)
    response.raise_for_status()
    return response.json()["results"][0]


def to_user_created_record(raw: dict[str, Any]) -> dict[str, Any]:
    """Map the randomuser.me payload onto our UserCreated Avro schema."""
    registered = datetime.fromisoformat(raw["registered"]["date"].replace("Z", "+00:00"))
    return {
        "id": str(uuid.uuid4()),
        "first_name": raw["name"]["first"],
        "last_name": raw["name"]["last"],
        "gender": raw["gender"],
        "email": raw["email"],
        "username": raw["login"]["username"],
        "country": raw["location"]["country"],
        "post_code": str(raw["location"]["postcode"]),
        "phone": raw["phone"],
        "picture_url": raw["picture"]["medium"],
        "registered_date": registered,
        "created_at": datetime.now(UTC),
    }


def build_producer(settings: ProducerSettings) -> SerializingProducer:
    schema_registry_client = SchemaRegistryClient({"url": settings.schema_registry_url})
    avro_serializer = AvroSerializer(
        schema_registry_client,
        load_schema_str(),
        lambda record, ctx: record,
    )

    return SerializingProducer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        log.error("Delivery failed for record %s: %s", msg.key(), err)
    else:
        log.debug("Delivered to %s [%s]", msg.topic(), msg.partition())


def main() -> None:
    settings = ProducerSettings()
    log.info(
        "Starting producer: topic=%s bootstrap=%s interval=%ss",
        settings.kafka_topic,
        settings.kafka_bootstrap_servers,
        settings.fetch_interval_seconds,
    )

    producer = build_producer(settings)
    sent = 0

    try:
        while True:
            try:
                raw = fetch_raw_user(settings.randomuser_api_url)
                record = to_user_created_record(raw)
                producer.produce(
                    topic=settings.kafka_topic,
                    key=record["id"],
                    value=record,
                    on_delivery=delivery_report,
                )
                producer.poll(0)
                sent += 1
                if sent % 10 == 0:
                    log.info("Sent %s records so far", sent)
            except requests.RequestException as e:
                log.warning("API fetch failed: %s -- retrying after backoff", e)
                time.sleep(5)
            except Exception:
                log.exception("Unexpected error producing record")
                time.sleep(5)

            time.sleep(settings.fetch_interval_seconds)
    except KeyboardInterrupt:
        log.info("Shutdown requested, flushing producer...")
    finally:
        producer.flush(10)
        log.info("Producer stopped, sent %s records total", sent)


if __name__ == "__main__":
    main()
