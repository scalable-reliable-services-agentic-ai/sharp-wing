import os
import json
import random
import logging
import asyncio
from aiokafka import AIOKafkaProducer
from src.generator.transaction import Transaction

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Environment & Constants ---
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC = os.environ["KAFKA_RAW_TRANSACTIONS_TOPIC"]


# --- Connection Handlers ---
async def get_kafka_producer():
    while True:
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()
            logging.info("AIOKafkaProducer connected.")
            return producer
        except Exception as e:
            logging.error(f"Could not connect to Kafka: {e}. Retrying...")
            await asyncio.sleep(5)


# --- Main Application ---
async def main():
    producer = await get_kafka_producer()
    logging.info(f"Generator starting, producing to topic '{KAFKA_TOPIC}'.")

    try:
        while True:
            try:
                # Generate and send transaction
                transaction = Transaction()
                transaction_data = transaction.get_kafka_message()
                await producer.send_and_wait(KAFKA_TOPIC, transaction_data)
                logging.info(
                    f"Produced transaction: {transaction_data['transaction_id']}"
                )

                # Sleep for a random interval
                min_sleep = int(os.environ["GENERATOR_MIN_SLEEP_MS"]) / 1000.0
                max_sleep = int(os.environ["GENERATOR_MAX_SLEEP_MS"]) / 1000.0
                await asyncio.sleep(random.uniform(min_sleep, max_sleep))

            except Exception as e:
                logging.error(f"An error occurred in the main loop: {e}")
                await asyncio.sleep(5)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
