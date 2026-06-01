import json
import time
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as aioredis
from src.config import settings, configure_logging
from prometheus_client import Counter, Gauge
from collections import deque
from src.prometheus_metrics.metrics import start_metrics_server

logger = configure_logging(__name__)


# Metrics Definition
APP_TRANSACTIONS_CNT_VALID_OUT = Counter(
    "app_tfd_valid_transactions", "Number of transactions that passed validation"
)
APP_TRANSACTIONS_CNT_INVALID_OUT = Counter(
    "app_tfd_invalid_transactions", "Number of transactions that failed validation"
)
APP_TRANSACTIONS_CNT_TOTAL_VALIDATED_IN = Counter(
    "app_tfd_total_validated_transactions",
    "Total number of transactions that has been validated",
)

APP_TRANSACTIONS_AVERAGE_INCOMING_TPS = Gauge(
    "app_tfd_average_transactions_per_second",
    "Average number of incoming transactions per second",
)
recent_timestamps = deque()


# Environment Variables
KAFKA_BROKER = settings.kafka_broker
IN_TOPIC = settings.kafka_raw_transactions_topic
OUT_TOPIC_VALID = settings.kafka_validated_transactions_topic
OUT_TOPIC_INVALID = settings.kafka_invalid_transactions_topic
REDIS_HOST = settings.redis_host


# Connection Handlers
async def get_kafka_consumer():
    while True:
        try:
            consumer = AIOKafkaConsumer(
                IN_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                group_id="validator-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            await consumer.start()
            logger.info("AIOKafkaConsumer connected.")
            return consumer
        except Exception as e:
            logger.error(
                f"Could not connect to Kafka Consumer: {e}. Retrying...", exc_info=True
            )
            await asyncio.sleep(5)


async def get_kafka_producer():
    while True:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()
            logger.info("AIOKafkaProducer connected.")
            return producer
        except Exception as e:
            logger.error(
                f"Could not connect to Kafka Producer: {e}. Retrying...", exc_info=True
            )
            await asyncio.sleep(5)


async def get_redis_connection():
    while True:
        try:
            r = aioredis.from_url(f"redis://{REDIS_HOST}", decode_responses=True)
            await r.ping()
            logger.info("Async Redis connection established.")
            return r
        except Exception as e:
            logger.error(f"Could not connect to Redis: {e}. Retrying...", exc_info=True)
            await asyncio.sleep(5)


def _is_valid_amount(
    amount: int | float, min_val: int | float, max_val: int | float
) -> bool:
    return min_val < amount < max_val


def is_valid_check(amount: int | float) -> tuple[bool, str]:
    min_amount = settings.validator_min_amount
    max_amount = settings.validator_max_amount

    isvalid = _is_valid_amount(amount, min_amount, max_amount)
    reason = (
        "" if isvalid else f"Amount outside of valid range ({min_amount}-{max_amount})"
    )

    return isvalid, reason


async def update_tps_gauge():
    while True:
        now = time.time()

        while recent_timestamps and recent_timestamps[0] < now - 60:
            recent_timestamps.popleft()

        APP_TRANSACTIONS_AVERAGE_INCOMING_TPS.set(len(recent_timestamps) / 60.0)

        await asyncio.sleep(1)


# Core Logic
async def process_message(message, producer, redis_client):
    try:
        transaction = message.value

        transaction["current_state"] = "received"
        transaction["history"] = {}

        # Real-time Client Profile Update
        sender_id = transaction["sender_id"]
        amount = transaction["amount"]

        recent_timestamps.append(time.time())
        APP_TRANSACTIONS_CNT_TOTAL_VALIDATED_IN.inc()

        pipe = redis_client.pipeline()
        pipe.lpush(f"client:{sender_id}:amounts", amount)
        pipe.ltrim(f"client:{sender_id}:amounts", 0, 4)
        pipe.hset(
            f"client:{sender_id}:profile",
            mapping={
                "last_location": transaction["location"],
                "last_ip": transaction["ip_address"],
                "last_seen_ts": transaction["timestamp_ms"],
            },
        )
        await pipe.execute()

        is_valid, reason = is_valid_check(amount=transaction["amount"])

        # Routing
        if is_valid:
            transaction["current_state"] = "clean"
            transaction["history"]["validator"] = {
                "decision": "clean",
                "ts": int(time.time() * 1000),
            }
            await producer.send_and_wait(OUT_TOPIC_VALID, transaction)
            await redis_client.incr("total_validated_realtime")
            APP_TRANSACTIONS_CNT_VALID_OUT.inc()
            logger.info(f"Validated transaction {transaction['transaction_id']}: OK")

        else:
            transaction["current_state"] = "invalid"
            transaction["history"]["validator"] = {
                "decision": "invalid",
                "reason": reason,
                "ts": int(time.time() * 1000),
            }
            await producer.send_and_wait(OUT_TOPIC_INVALID, transaction)

            pipe = redis_client.pipeline()
            pipe.incr("total_invalid_realtime")
            pipe.lpush("recent_invalid_transactions", json.dumps(transaction))
            pipe.ltrim("recent_invalid_transactions", 0, 99)

            await pipe.execute()
            APP_TRANSACTIONS_CNT_INVALID_OUT.inc()

            logger.warning(
                f"Validated transaction {transaction['transaction_id']}: INVALID - {reason}"
            )

    except json.JSONDecodeError as e:
        logger.error(
            f"Failed to decode message: {message.value}. Error: {e}", exc_info=True
        )
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while processing message: {e}", exc_info=True
        )


# Main Application Runner
async def main():
    # Initialize connections
    consumer = await get_kafka_consumer()
    producer = await get_kafka_producer()
    redis_client = await get_redis_connection()

    logger.info(f"Validator starting. Consuming from topic: {IN_TOPIC}")

    try:
        asyncio.create_task(update_tps_gauge())

        async for message in consumer:
            # Create a non-blocking task to process each message
            asyncio.create_task(process_message(message, producer, redis_client))
    finally:
        await consumer.stop()
        await producer.stop()
        await redis_client.close()


if __name__ == "__main__":
    start_metrics_server(8001)
    asyncio.run(main())
