import time
import json
import asyncio
from aiokafka import AIOKafkaConsumer
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from prometheus_client import Counter
from src.prometheus_metrics.metrics import start_metrics_server
from src.config import settings, configure_logging
from src.database.models import Base, Transaction

logger = configure_logging(__name__)

BATCH_SIZE = settings.batch_size
BATCH_INTERVAL = settings.batch_interval
KAFKA_BROKER = settings.kafka_broker
DATABASE_URL = settings.async_database_url

# Environment and Constants
TOPICS = [
    settings.kafka_noanomaly_transactions_topic,
    settings.kafka_final_transactions_topic,
    settings.kafka_human_review_required_topic
]

# Prometheus Metrics Definition
APP_TRANSACTIONS_CNT_INSERTED_DB = Counter(
    "app_tfd_inserted_transactions",
    "Number of transactions inserted/ingested into the database",
)


async def get_db_engine():
    while True:
        try:
            engine = create_async_engine(DATABASE_URL)
            async with engine.connect():
                logger.info("Database connection established successfully")
                return engine
        except Exception as e:
            logger.error(f"Could not connect to database: {e}. Retrying in 5 seconds...", exc_info=True)
            await asyncio.sleep(5)


async def setup_database(engine):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Table 'transactions' schema created or already exists.")
    try:
        async with engine.connect() as connection:
            await connection.execute(
                text(
                    "SELECT create_hypertable('transactions', 'timestamp_ms', if_not_exists => TRUE, chunk_time_interval => 86400000);")
            )
            await connection.commit()
    except Exception as e:
        logger.error(f"Error setting up hypertable: {e}", exc_info=True)


async def get_kafka_consumer():
    while True:
        try:
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
            logger.error(f"Could not connect to Kafka consumer: {e}. Retrying...", exc_info=True)
            await asyncio.sleep(5)


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

                records_found_this_poll = 0

                for tp, messages in result.items():
                    for message in messages:
                        tx_data = message.value
                        topic_name = tp.topic
                        records_found_this_poll += 1
                        # APP_TRANSACTIONS_CNT_INSERTED_DB.inc()

                        # Extract routing and score indicators (Preserved from Your Branch)
                        s1_routing = tx_data.pop("system_1_routing", None)
                        s1_ml_score = tx_data.pop("system_1_ml_score", None)

                        # Advanced Dynamic Audit State Resolution Engine (Preserved from Your Branch)
                        if topic_name == settings.kafka_human_review_required_topic:
                            tx_data["current_state"] = "ESCALATED"
                        elif topic_name == settings.kafka_final_transactions_topic:
                            if s1_routing == "ML_AUTO_DENIED":
                                tx_data["current_state"] = "AUTO_DENIED"
                            else:
                                tx_data["current_state"] = "AI_RESOLVED"
                        else:
                            if s1_routing == "ML_AUTO_APPROVED":
                                tx_data["current_state"] = "AUTO_APPROVED"
                            else:
                                tx_data["current_state"] = "CLEARED_SYSTEM_1"

                        # Extract the reasoning dictionaries
                        ai_eval = tx_data.pop("agentic_evaluation", {})
                        sys1_reasons = tx_data.pop("system_1_reasons", [])
                        obs_eval = tx_data.pop("observer_evaluation", {})

                        if "history" not in tx_data or not isinstance(tx_data["history"], dict):
                            tx_data["history"] = {}

                        # Append historical traces seamlessly
                        if sys1_reasons:
                            tx_data["history"]["system_1"] = sys1_reasons
                        if ai_eval:
                            tx_data["history"]["system_2"] = ai_eval
                        if obs_eval:
                            tx_data["history"]["observer_evaluation"] = obs_eval
                        if s1_ml_score is not None:
                            tx_data["history"]["system_1_ml_score"] = s1_ml_score

                        buffer.append(tx_data)

                # Increment Prometheus database ingestion counter using exact poll sizes
                if records_found_this_poll > 0:
                    APP_TRANSACTIONS_CNT_INSERTED_DB.inc(records_found_this_poll)

                time_since_last_flush = time.time() - last_flush_time
                if len(buffer) >= BATCH_SIZE or (time_since_last_flush > BATCH_INTERVAL and buffer):
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
                logger.error(f"An error occurred during the batch insert loop: {e}", exc_info=True)
                buffer = []
    finally:
        await consumer.stop()
        await engine.dispose()


if __name__ == "__main__":
    # Open metrics server on port 8004 for Prometheus mapping
    start_metrics_server(8004)
    asyncio.run(main())
