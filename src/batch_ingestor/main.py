import os
import time
import logging
import json
import asyncio
from aiokafka import AIOKafkaConsumer
from sqlalchemy import text, Column, BigInteger, String, Float
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from src.database.models import Base, Transaction

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Environment Variables ---
BATCH_SIZE = int(os.environ["BATCH_SIZE"])
BATCH_INTERVAL = int(os.environ["BATCH_INTERVAL"])
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC = os.environ["KAFKA_FINAL_TRANSACTIONS_TOPIC"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"


# --- Connection & Setup ---
async def get_db_engine():
    while True:
        try:
            engine = create_async_engine(DATABASE_URL)
            async with engine.connect() as connection:
                logging.info("Database connection established successfully.")
                return engine
        except Exception as e:
            logging.error(
                f"Could not connect to database: {e}. Retrying in 5 seconds..."
            )
            await asyncio.sleep(5)


async def setup_database(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logging.info("Table 'transactions' schema created or already exists.")
    try:
        async with engine.connect() as connection:
            await connection.execute(
                text(
                    "SELECT create_hypertable('transactions', 'timestamp_ms', if_not_exists => TRUE, chunk_time_interval => 86400000);"
                )
            )
            await connection.commit()
            logging.info("Ensured 'transactions' is a hypertable.")
    except Exception as e:
        logging.warning(
            f"Failed to create hypertable (this is often fine if it already exists): {e}"
        )


async def get_kafka_consumer():
    while True:
        try:
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                group_id="batch-ingestor-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            await consumer.start()
            logging.info(f"Consumer connected to Kafka topic: {KAFKA_TOPIC}")
            return consumer
        except Exception as e:
            logging.error(f"Could not connect to Kafka consumer: {e}. Retrying...")
            await asyncio.sleep(5)


# --- Main Application ---
async def main():
    consumer = await get_kafka_consumer()
    engine = await get_db_engine()
    await setup_database(engine)

    buffer = []
    last_flush_time = time.time()
    logging.info("Starting main processing loop...")
    try:
        while True:
            try:
                # Poll for messages with a timeout
                result = await consumer.getmany(timeout_ms=1000, max_records=BATCH_SIZE)
                for tp, messages in result.items():
                    for message in messages:
                        message.value.pop("status", None)
                        buffer.append(message.value)

                time_since_last_flush = time.time() - last_flush_time
                if len(buffer) >= BATCH_SIZE or (
                    time_since_last_flush > BATCH_INTERVAL and buffer
                ):
                    if not buffer:
                        continue

                    async with engine.connect() as connection:
                        stmt = pg_insert(Transaction).values(buffer)
                        stmt = stmt.on_conflict_do_nothing(
                            index_elements=["transaction_id", "timestamp_ms"]
                        )
                        await connection.execute(stmt)
                        await connection.commit()

                    logging.info(f"Flushed {len(buffer)} records to the database.")
                    buffer = []
                    last_flush_time = time.time()

            except Exception as e:
                logging.error(f"An error occurred during the batch insert loop: {e}")
                buffer = []  # Clear buffer on error
    finally:
        await consumer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
