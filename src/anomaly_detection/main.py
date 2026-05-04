import json
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as aioredis
from src.config import settings, configure_logging
from datetime import datetime

logger = configure_logging(__name__)

# Funnel Topics
IN_TOPIC = settings.kafka_validated_transactions_topic
OUT_SAFE_TOPIC = settings.kafka_noanomaly_transactions_topic
OUT_ANOMALY_TOPIC = settings.kafka_anomaly_detected_transactions_topic


async def check_rules(transaction: dict, redis_client: aioredis.Redis) -> tuple[bool, list[str]]:
    """Evaluates deterministic rules tailored to the seeder personas"""
    reasons = []
    is_anomaly = False

    amount = float(transaction.get("amount", 0.0))
    client_id = transaction.get("client_id")
    timestamp_str = transaction.get("timestamp_iso", "")
    location = transaction.get("location", "")

    # Extract the hour from the timestamp for behavioral checks
    try:
        tx_hour = datetime.fromisoformat(timestamp_str).hour
    except ValueError:
        tx_hour = 12 # Default fallback

    # RULE 1: Smurfing Check (Catches Fraud_Smurfing)
    if 9900 <= amount <= 9999:
        is_anomaly = True
        reasons.append(f"Smurfing Check: Amount ${amount} is designed to evade 10k reporting.")

    # RULE 2: Time-Based Massive Drain (Catches Fraud_ATO)
    # VIPs/Corporate do large amounts, but NOT at 3 AM
    if amount >= 20000 and (tx_hour <= 4 or tx_hour >= 23):
        is_anomaly = True
        reasons.append(f"ATO Check: Massive transfer (${amount}) initiated at suspicious hour ({tx_hour}:00).")

    # RULE 3: Geolocation Tripwire (Catches StolenCard & ImpossibleTravel)
    # If it's not a standard IT/US coordinate, flag it for LLM review
    if "SE_ASIA" in location or "GLOBAL" in location:
         is_anomaly = True
         reasons.append(f"Location Tripwire: Transaction from high-risk or unexpected region ({location}). Needs LLM review.")

    # RULE 4: Standard Velocity Check
    if client_id:
        redis_key = f"velocity:{client_id}"
        current_count = await redis_client.incr(redis_key)
        if current_count == 1:
            await redis_client.expire(redis_key, 60) # 60 seconds window

        if current_count > 4:
            is_anomaly = True
            reasons.append(f"Velocity Check: User {client_id} attempted {current_count} transactions in 60s.")

    return is_anomaly, reasons


async def process_message(message, producer: AIOKafkaProducer, redis_client: aioredis.Redis):
    transaction = message.value
    tx_id = transaction.get("transaction_id", "UNKNOWN")

    try:
        # 1. Check the fast rules
        is_anomaly, reasons = await check_rules(transaction, redis_client)

        # 2. Route the transaction to the correct topic funnel
        if is_anomaly:
            logger.warning(f"Rule triggered [TX: {tx_id}] - {reasons}")
            transaction["system_1_reasons"] = reasons
            await producer.send_and_wait(OUT_ANOMALY_TOPIC, transaction)
        else:
            logger.info(f"Safe [TX: {tx_id}]")
            await producer.send_and_wait(OUT_SAFE_TOPIC, transaction)

    except Exception as e:
        logger.error(f"Error processing transaction {tx_id}: {e}")


async def main():
    # Setup Kafka and Redis connections
    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=settings.kafka_broker,
        group_id="deterministic-anomaly-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest"
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    redis_client = aioredis.from_url(f"redis://{settings.redis_host}:{settings.redis_port}", decode_responses=True)

    await consumer.start()
    await producer.start()
    logger.info(f"Fast System 1 Anomaly Detection started. Listening on: {IN_TOPIC}")

    try:
        async for message in consumer:
            # Because Redis takes 1 millisecond, we can safely use create_task for massive speed
            asyncio.create_task(process_message(message, producer, redis_client))
    finally:
        await consumer.stop()
        await producer.stop()
        await redis_client.close()


if __name__ == "__main__":
    asyncio.run(main())
