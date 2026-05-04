import time
import json
import asyncio
from aiokafka import AIOKafkaConsumer
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from src.config import settings, configure_logging
from src.database.models import Base, Transaction

logger = configure_logging(__name__)

BATCH_SIZE = settings.batch_size
BATCH_INTERVAL = settings.batch_interval
KAFKA_BROKER = settings.kafka_broker
DATABASE_URL = settings.async_database_url

TOPICS = [
    settings.kafka_noanomaly_transactions_topic,
    settings.kafka_final_transactions_topic,
    settings.kafka_human_review_required_topic,
]


async def get_db_engine():
    while True:
        try:
            engine = create_async_engine(DATABASE_URL)
            async with engine.connect():
                logger.info("Database connection established successfully")
                return engine
        except Exception as e:
            logger.error(
                f"Could not connect to database: {e}. Retrying in 5 seconds..."
            )
            await asyncio.sleep(5)


async def setup_database(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Table 'transactions' schema created or already exists.")
    try:
        async with engine.connect() as connection:
            await connection.execute(
                text(
                    "SELECT create_hypertable('transactions', 'timestamp_ms', if_not_exists => TRUE, chunk_time_interval => 86400000);"
                )
            )
            await connection.commit()
    except Exception as e:
        logger.error(f"Error setting up hypertable: {e}")


async def get_kafka_consumer():
    while True:
        try:
            # We now pass the TOPICS list directly
            consumer = AIOKafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BROKER,
                auto_offset_reset="earliest",
                group_id="batch-ingestor-group",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            )
            await consumer.start()
            logger.info(f"Ingestor connected to End-State Topics: {TOPICS}")
            return consumer
        except Exception as e:
            logger.error(f"Could not connect to Kafka consumer: {e}. Retrying...")
            await asyncio.sleep(5)


# Main Application
async def main():
    consumer = await get_kafka_consumer()
    engine = await get_db_engine()
    await setup_database(engine)

    buffer = []
    last_flush_time = time.time()
    logger.info("Starting main processing loop...")
    try:
        while True:
            try:
                result = await consumer.getmany(timeout_ms=1000, max_records=BATCH_SIZE)
                for tp, messages in result.items():
                    for message in messages:
                        tx_data = message.value

                        # AI Data Extraction & Status Routing
                        topic_name = tp.topic

                        if topic_name == settings.kafka_human_review_required_topic:
                            tx_data["current_state"] = "ESCALATED"
                        elif topic_name == settings.kafka_final_transactions_topic:
                            tx_data["current_state"] = "AI_RESOLVED"
                        else:
                            tx_data["current_state"] = "CLEARED_SYSTEM_1"

                        # Extract the reasoning dictionaries
                        ai_eval = tx_data.pop("agentic_evaluation", {})
                        sys1_reasons = tx_data.pop("system_1_reasons", [])

                        # Safely ensure history is a dictionary (in case it's missing)
                        if "history" not in tx_data or not isinstance(
                            tx_data["history"], dict
                        ):
                            tx_data["history"] = {}

                        # ADD to the existing dictionary so we don't erase the validator's logs
                        if sys1_reasons:
                            tx_data["history"]["system_1"] = sys1_reasons
                        if ai_eval:
                            tx_data["history"]["system_2"] = ai_eval

                        buffer.append(tx_data)

                time_since_last_flush = time.time() - last_flush_time
                if len(buffer) >= BATCH_SIZE or (
                    time_since_last_flush > BATCH_INTERVAL and buffer
                ):
                    async with engine.connect() as connection:
                        stmt = pg_insert(Transaction).values(buffer)
                        stmt = stmt.on_conflict_do_nothing(
                            index_elements=["transaction_id", "timestamp_ms"]
                        )
                        await connection.execute(stmt)
                        await connection.commit()

                    logger.info(f"Flushed {len(buffer)} records to the database.")
                    buffer = []
                    last_flush_time = time.time()

            except Exception as e:
                logger.error(f"An error occurred during the batch insert loop: {e}")
                buffer = []
    finally:
        await consumer.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
