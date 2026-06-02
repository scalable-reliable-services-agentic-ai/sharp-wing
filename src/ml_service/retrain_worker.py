import json
import asyncio
import os
from aiokafka import AIOKafkaConsumer
from src.config import settings, configure_logging
from src.ml_service.train import fetch_combined_training_data, preprocess_and_train

logger = configure_logging(__name__)

HUMAN_RESOLVED_TOPIC = getattr(settings, "kafka_human_resolved_topic", "fraud_human_resolved")
RETRAIN_BATCH_THRESHOLD = 10
CHALLENGER_MODEL_PATH = "/app/src/ml_service/models/challenger_model.pkl"

async def main():
    consumer = AIOKafkaConsumer(
        HUMAN_RESOLVED_TOPIC,
        bootstrap_servers=settings.kafka_broker,
        group_id="ml-retraining-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="latest"
    )

    await consumer.start()
    logger.info(f"MLOps Pipeline Retraining loop activated. Subscribed to: {HUMAN_RESOLVED_TOPIC}")

    resolution_counter = 0

    try:
        async for message in consumer:
            payload = message.value
            tx_id = payload.get("transaction_id", "UNKNOWN")

            resolution_counter += 1
            logger.info(f"Received HITL resolution update ({resolution_counter}/{RETRAIN_BATCH_THRESHOLD}) for TX: {tx_id}")

            if resolution_counter >= RETRAIN_BATCH_THRESHOLD:
                logger.info("Training batch target reached. Initiating XGBoost model optimization...")
                try:
                    updated_rows = await fetch_combined_training_data()

                    # Pass a dedicated destination path so we do not overwrite the baseline model
                    await asyncio.to_thread(
                        preprocess_and_train,
                        updated_rows,
                        model_output_path=CHALLENGER_MODEL_PATH
                    )

                    logger.info(f"Challenger model generated successfully at: {CHALLENGER_MODEL_PATH}")
                    resolution_counter = 0
                except Exception as e:
                    logger.error(f"Automated model retraining evolution encountered an error: {e}")

    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
