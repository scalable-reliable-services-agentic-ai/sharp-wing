import json
import time
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as aioredis
from src.config import settings, configure_logging

logger = configure_logging(__name__)

# --- Environment Variables ---
KAFKA_BROKER = settings.kafka_broker
IN_TOPIC = settings.kafka_raw_transactions_topic
FINAL_TOPIC = settings.kafka_final_transactions_topic
REDIS_HOST = settings.redis_host


# --- Connection Handlers ---
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
            logger.error(f"Could not connect to Kafka Consumer: {e}. Retrying...")
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
            logger.error(f"Could not connect to Kafka Producer: {e}. Retrying...")
            await asyncio.sleep(5)


async def get_redis_connection():
    while True:
        try:
            r = aioredis.from_url(f"redis://{REDIS_HOST}", decode_responses=True)
            await r.ping()
            logger.info("Async Redis connection established.")
            return r
        except Exception as e:
            logger.error(f"Could not connect to Redis: {e}. Retrying...")
            await asyncio.sleep(5)


# --- Core Logic ---
async def process_message(message, producer, redis_client):
    try:
        transaction = message.value

        # Add initial state and history
        transaction["current_state"] = "received"
        transaction["history"] = []

        # --- Real-time Client Profile Update ---
        client_id = transaction["client_id"]
        amount = transaction["amount"]
        # Use a pipeline for atomic operations
        pipe = redis_client.pipeline()
        pipe.lpush(f"client:{client_id}:amounts", amount)
        pipe.ltrim(f"client:{client_id}:amounts", 0, 4)
        pipe.hset(
            f"client:{client_id}:profile",
            mapping={
                "last_location": transaction["location"],
                "last_ip": transaction["ip_address"],
                "last_seen_ts": transaction["timestamp_ms"],
            },
        )
        await pipe.execute()

        # --- Validation Logic ---
        min_amount = settings.validator_min_amount
        max_amount = settings.validator_max_amount
        is_valid = min_amount < transaction["amount"] < max_amount
        reason = (
            ""
            if is_valid
            else f"Amount outside of valid range ({min_amount}-{max_amount})"
        )

        # --- Routing ---
        if is_valid:
            transaction["current_state"] = "clean"
            transaction["history"].append(
                {
                    "service": "validator",
                    "decision": "clean",
                    "ts": int(time.time() * 1000),
                }
            )
            await producer.send_and_wait(FINAL_TOPIC, transaction)
            await redis_client.incr("total_validated_realtime")
            logger.info(f"Validated transaction {transaction['transaction_id']}: OK")
        else:
            transaction["current_state"] = "invalid"
            transaction["history"].append(
                {
                    "service": "validator",
                    "decision": "invalid",
                    "reason": reason,
                    "ts": int(time.time() * 1000),
                }
            )
            await producer.send_and_wait(FINAL_TOPIC, transaction)
            pipe = redis_client.pipeline()
            pipe.incr("total_invalid_realtime")
            pipe.lpush("recent_invalid_transactions", json.dumps(transaction))
            pipe.ltrim("recent_invalid_transactions", 0, 99)
            await pipe.execute()
            logger.warning(
                f"Validated transaction {transaction['transaction_id']}: INVALID - {reason}"
            )

    except json.JSONDecodeError as e:
        logger.error(f"Failed to decode message: {message.value}. Error: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while processing message: {e}")


# --- Main Application Runner ---
async def main():
    # Initialize connections
    consumer = await get_kafka_consumer()
    producer = await get_kafka_producer()
    redis_client = await get_redis_connection()

    logger.info(f"Validator starting. Consuming from topic: {IN_TOPIC}")
    try:
        async for message in consumer:
            # Create a non-blocking task to process each message
            asyncio.create_task(process_message(message, producer, redis_client))
    finally:
        await consumer.stop()
        await producer.stop()
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())
