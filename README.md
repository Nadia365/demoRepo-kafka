# Kafka + Postgres demo (learning version)

This mini project shows a very simple event-driven flow:

1. The **producer** creates a `dataset.created` event.
2. Kafka stores that event inside the `data-layer-events` topic.
3. The **consumer** reads the event.
4. The consumer writes the event into **Postgres**.

## Why this version is useful for learning

This version keeps the code small, but adds more meaningful logs so you can understand what is happening at each step.

## Main files

- `docker-compose.yml` → starts Kafka, Postgres, Kafka UI, producer, and consumer
- `producer/producer.py` → sends one event to Kafka
- `consumer/consumer.py` → reads the event and stores it in Postgres
- `init.sql` → creates the `dataset_events` table

## Run

```bash
docker compose up --build
```

## What to look for in the logs

### Producer
- waiting for Kafka
- preparing the event payload
- event stored in Kafka
- flush completed

### Consumer
- connected to Postgres
- subscribed to topic
- message received from Kafka
- decoded payload
- event saved to Postgres
- offset committed

## Why `flush()` matters

Kafka producers buffer messages for performance. `flush()` makes sure buffered messages are sent before the script exits.

## Why offset commit matters

The offset is Kafka's way of remembering which messages a consumer already processed.
When the consumer commits the offset, it tells Kafka: "I handled this message successfully."
