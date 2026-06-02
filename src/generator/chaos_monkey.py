import json
import asyncio
import random
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
from src.config import settings, configure_logging

logger = configure_logging(__name__)

TARGET_TOPIC = settings.kafka_validated_transactions_topic


def build_payload(sender_id: int, amount: float, location: str) -> dict:
    """Helper to ensure payloads perfectly match the database schema"""
    return {
        "transaction_id": str(random.randint(100_000_000, 999_999_999)),
        "sender_id": sender_id,
        "receiver_id": random.randint(100_000, 999_999),
        "amount": amount,
        "currency": "USD",
        "location": location,
        "transaction_type": "TRANSFER",
        "channel": "API",
        "timestamp_iso": datetime.now(timezone.utc).isoformat(),
        "timestamp_ms": int(datetime.now(timezone.utc).timestamp() * 1000)
    }


async def launch_chaos_attack():
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()

    logger.warning("[CHAOS MONKEY ACTUATED] Preparing to flood the pipeline in 3 seconds...")
    await asyncio.sleep(3)

    # VECTOR A: THE SMURFING BOTNET (Tests LLM Velocity Tool + Deterministic)
    logger.error("EXECUTING ATTACK VECTOR A: Smurfing Cluster (Targeting Reporting Limits)...")
    smurf_sender_id = 999001

    for i in range(4):
        payload = build_payload(
            sender_id=smurf_sender_id,
            amount=round(random.uniform(9910.0, 9980.0), 2),  # Trips the 9900-9999 rule
            location="41.8902, 12.4922"  # Rome (Safe Geo)
        )
        await producer.send(TARGET_TOPIC, payload)
        await asyncio.sleep(8.0)  # Paced for LLM breathing room

    # VECTOR B: THE QUANTUM JUMPER (Tests LLM Impossible Travel Tool)
    logger.error("EXECUTING ATTACK VECTOR B: Geographic Impossibility...")
    jumper_sender_id = 999002

    # Hit 1: Located in Milan, Italy (Safe Geo)
    payload_milan = build_payload(jumper_sender_id, 2500.0, "45.4642, 9.1900")
    await producer.send(TARGET_TOPIC, payload_milan)

    # 5 Seconds later... Hit 2 from Los Angeles, USA! (Safe Geo, but impossible velocity)
    await asyncio.sleep(5.0)
    payload_la = build_payload(jumper_sender_id, 22000.0, "34.0522, -118.2437")
    await producer.send(TARGET_TOPIC, payload_la)

    await asyncio.sleep(8.0)

    # VECTOR C: THE ML STRESS TEST (Bypasses Deterministic, Tests XGBoost)
    logger.error("EXECUTING ATTACK VECTOR C: ML Sieve Stress Test...")
    ml_sender_id = 999003

    # Payload designed to BYPASS deterministic rules:
    # 1. Amount is $85,000 (Not smurfing, >20k but we will assume daytime execution)
    # 2. Location is New York (Safe US Geo bounds)
    # 3. First time seen (No velocity triggers)
    # This forces it into the ML Module to see how XGBoost handles high-value edge cases.
    payload_ml = build_payload(ml_sender_id, 85000.0, "40.7128, -74.0060")
    await producer.send(TARGET_TOPIC, payload_ml)

    logger.warning("Chaos burst completely deployed into Kafka. Monitor the React Dashboard!")
    await producer.stop()


if __name__ == "__main__":
    asyncio.run(launch_chaos_attack())
