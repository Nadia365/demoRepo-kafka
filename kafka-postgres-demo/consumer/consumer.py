import json
import logging
import time
from typing import Any

import psycopg2
from confluent_kafka import Consumer
from psycopg2.extensions import connection as PgConnection

TOPIC = "data-layer-events"
BOOTSTRAP_SERVERS = "kafka:9092"
CONSUMER_GROUP = "data-layer-consumer-group"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | consumer | %(message)s",
)
logger = logging.getLogger(__name__)


def get_db_connection() -> PgConnection:
    while True:
        try:
            conn = psycopg2.connect(
                host="postgres",
                dbname="demo",
                user="demo",
                password="demo",
            )
            logger.info("Connected to Postgres successfully.")
            return conn
        except Exception as exc:
            logger.warning("Postgres is not ready yet: %s", exc)
            time.sleep(3)


def insert_event(conn: PgConnection, payload: dict[str, str]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dataset_events (event_id, event_type, dataset_id, dataset_name)
            VALUES (%s, %s, %s, %s)
            """,
            (
                payload["event_id"],
                payload["event_type"],
                payload["dataset_id"],
                payload["dataset_name"],
            ),
        )
    conn.commit()
    logger.info(
        "Event saved to Postgres | event_id=%s dataset_id=%s",
        payload["event_id"],
        payload["dataset_id"],
    )


def main() -> None:
    logger.info("Waiting for Kafka and Postgres to become ready...")
    time.sleep(15)

    conn = get_db_connection()

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    consumer.subscribe([TOPIC])
    logger.info("Subscribed to topic: %s", TOPIC)
    logger.info(
        "The consumer will keep polling Kafka. When a new dataset event arrives, it will write it into Postgres."
    )

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            logger.info(
                "Message received from Kafka | topic=%s partition=%s offset=%s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

            payload = json.loads(msg.value().decode("utf-8"))
            logger.info("Decoded payload: %s", payload)
            logger.info(
                "Meaning of this event: another service announced that a dataset was created."
            )

            insert_event(conn, payload)
            consumer.commit(message=msg)
            logger.info("Offset committed. Kafka now knows this message was processed.")

    except KeyboardInterrupt:
        logger.info("Stopping consumer gracefully...")
    finally:
        consumer.close()
        conn.close()
        logger.info("Consumer and database connection closed.")


if __name__ == "__main__":
    main()
