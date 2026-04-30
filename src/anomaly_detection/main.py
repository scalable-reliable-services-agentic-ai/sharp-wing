import json
import time
import asyncio
from pathlib import Path
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as aioredis
from src.config import settings, configure_logging

# from litellm import acompletion
from openai import OpenAI
from mcp import (
    ClientSession,
    StdioServerParameters,
)  # lub HTTP, zależnie jak wystawiony jest MCP
from mcp.client.stdio import stdio_client

logger = configure_logging(__name__)


MODEL_PROXY = settings.litellm_proxy_url
MODEL_KEY = settings.litellm_api_key
# MODEL_NAME = settings.litellm_mistral_model
MODEL_NAME = settings.litellm_gemini_model

PROMPT_DIR = Path(__file__).parent / "prompts"
SYSTEM_PROMPT = (PROMPT_DIR / "system.md").read_text()
logger.info("System prompt loaded successfully.")


# --- Environment Variables ---
KAFKA_BROKER = settings.kafka_broker
IN_TOPIC = settings.kafka_validated_transactions_topic
OUT_TOPIC = settings.kafka_final_transactions_topic
REDIS_HOST = settings.redis_host


async def get_kafka_consumer():
    while True:
        try:
            consumer = AIOKafkaConsumer(
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                group_id="anomaly-detection-group",
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


async def detect_anomaly(message, producer):
    """
    Processes a single transaction message to detect anomalies.
    """
    transaction_data = message.value
    logger.info(f"Processing transaction: {transaction_data.get('transaction_id')}")
    try:
        llm_client = OpenAI(
            api_key=MODEL_KEY,
            base_url=MODEL_PROXY,
        )

        response = llm_client.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(transaction_data)},
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
        )

        llm_output = json.loads(response.choices[0].message.content)
        logger.info(f"LLM analysis complete: {llm_output}")

        # await producer.send_and_wait(OUT_TOPIC, llm_output)
    except Exception as e:
        logger.error(f"Error during anomaly detection: {e}")


async def main():
    consumer = await get_kafka_consumer()
    producer = await get_kafka_producer()
    redis_client = await get_redis_connection()

    logger.info(f"Anomaly detection starting. Consuming from topic: {IN_TOPIC}")

    try:
        async for message in consumer:
            asyncio.create_task(detect_anomaly(message, producer, redis_client))
    finally:
        await consumer.stop()
        await producer.stop()
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())
