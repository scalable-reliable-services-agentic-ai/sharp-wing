import json
import asyncio
import random
from datetime import datetime, timezone
from aiokafka import AIOKafkaProducer
from src.config import settings, configure_logging

logger = configure_logging(__name__)

TARGET_TOPIC = settings.kafka_validated_transactions_topic


async def launch_chaos_attack():
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    await producer.start()

    logger.warning(
        "[CHAOS MONKEY ACTUATED] Preparing to flood the pipeline with malicious fraud patterns in 3 seconds...")
    await asyncio.sleep(3)

    # VECTOR A: THE SMURFING BOTNET
    logger.error("EXECUTING ATTACK VECTOR A: Smurfing Cluster (Targeting Reporting Limits)...")
    for i in range(15):
        tx_id = random.randint(100000000, 999999999)
        payload = {
            "transaction_id": str(tx_id),
            "sender_id": "attacker_bot_group_alpha",
            "amount": float(random.uniform(9910.0, 9980.0)),  # Explicitly sits inside $9900-$9999 tripwire rule
            "currency": "USD",
            "location": "41.8902, 12.4922",  # Rome
            "timestamp_iso": datetime.now(timezone.utc).isoformat()
        }
        await producer.send(TARGET_TOPIC, payload)
        await asyncio.sleep(0.1)  # Rapid high-frequency ingestion burst

    # VECTOR B: THE QUANTUM JUMPER (Impossible Travel Speed)
    logger.error("EXECUTING ATTACK VECTOR B: Geographic Impossibility (Account Takeover Bypass Test)...")
    victim_user = "compromised_corporate_user_88"

    # Hit 1: Located in Milan, Italy
    payload_milan = {
        "transaction_id": str(random.randint(100000000, 999999999)),
        "sender_id": victim_user,
        "amount": 2500.0,
        "currency": "EUR",
        "location": "45.4642, 9.1900",
        "timestamp_iso": datetime.now(timezone.utc).isoformat()
    }
    await producer.send(TARGET_TOPIC, payload_milan)

    # 200 Milliseconds later... Hit 2 from Los Angeles, USA!
    await asyncio.sleep(0.2)
    payload_la = {
        "transaction_id": str(random.randint(100000000, 999999999)),
        "sender_id": victim_user,
        "amount": 22000.0,  # Large amount to trigger System 2 analysis flags
        "currency": "USD",
        "location": "34.0522, -118.2437",
        "timestamp_iso": datetime.now(timezone.utc).isoformat()
    }
    await producer.send(TARGET_TOPIC, payload_la)

    logger.warning(
        "Chaos burst completely deployed into Kafka. Monitor your processing terminals and React Dashboard to evaluate mitigation stats")
    await producer.stop()


if __name__ == "__main__":
    asyncio.run(launch_chaos_attack())
