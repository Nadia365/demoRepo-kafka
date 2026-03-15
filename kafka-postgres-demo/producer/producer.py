import json
import logging
import time
from typing import Any

from confluent_kafka import Producer

TOPIC = "data-layer-events"
BOOTSTRAP_SERVERS = "kafka:9092"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | producer | %(message)s",
)
logger = logging.getLogger(__name__)


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error("Kafka could not store the event: %s", err)
        return

    logger.info(
        "Event stored in Kafka | topic=%s partition=%s offset=%s key=%s",
        msg.topic(),
        msg.partition(),
        msg.offset(),
        msg.key().decode("utf-8") if msg.key() else None,
    )


def build_event() -> dict[str, str]:
    return {
        "event_id": "evt-001",
        "event_type": "dataset.created",
        "dataset_id": "ds-001",
        "dataset_name": "customer_feedback",
    }


def main() -> None:
    logger.info("Waiting a few seconds so Kafka can finish starting up...")
    time.sleep(10)

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    event = build_event()

    logger.info("Preparing event payload: %s", event)
    logger.info(
        "This event means: a new dataset was created and other services can react to it asynchronously."
    )

    producer.produce(
        topic=TOPIC,
        key=event["dataset_id"],
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_report,
    )

    logger.info("Flushing buffered events so nothing is lost before the script exits...")
    producer.flush()
    logger.info("Producer finished sending the message.")


if __name__ == "__main__":
    main()
