import json
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as aioredis
from prometheus_client import Counter
from src.prometheus_metrics.metrics import start_metrics_server
from src.config import settings, configure_logging
from datetime import datetime
import math
import os
from src.ml_service.inference import FraudMLInference

logger = configure_logging(__name__)

# --- PROMETHEUS METRICS METRICS SETUP (Preserved from Teammate) ---
APP_TRANSACTIONS_CNT_FRAUD_ADC_OUT = Counter(
    "app_tfd_anomalous_transactions",
    "Number of transactions flagged as anomalous by rules",
)
APP_TRANSACTIONS_CNT_CLEAN_ADC_OUT = Counter(
    "app_tfd_safe_transactions", "Number of transactions that passed all rules"
)
APP_TRANSACTIONS_CNT_TOTAL_ADC_IN = Counter(
    "app_tfd_total_transactions_on_anomaly_detection_input",
    "Number of transactions received by simple non-ML anomaly detection service",
)

# Topics
IN_TOPIC = settings.kafka_validated_transactions_topic
OUT_SAFE_TOPIC = settings.kafka_noanomaly_transactions_topic
OUT_ANOMALY_TOPIC = settings.kafka_anomaly_detected_transactions_topic
# Directly route definitive ML decisions to the final persistence layer
OUT_FINAL_TOPIC = settings.kafka_final_transactions_topic

# Calibrated MLOps Sieve Gate Cut-offs
ML_AUTO_DENY_THRESHOLD = 0.80
ML_AUTO_APPROVE_THRESHOLD = 0.15

# Instantiated globally to maintain cached file handles across async loops
ml_engine = FraudMLInference()


def is_safe_location(lat: float, lon: float) -> bool:
    """Bounding boxes targeting baseline user persona locations (Italy / US)"""
    if (36.0 <= lat <= 48.0) and (6.0 <= lon <= 19.0):
        return True
    if (24.0 <= lat <= 50.0) and (-126.0 <= lon <= -65.0):
        return True
    return False


def calculate_speed_violation(loc1: str, loc2: str, time_window_seconds: float) -> bool:
    """Calculates whether velocity transitions exceed boundaries (>1000 km/h)"""
    if not loc1 or not loc2 or time_window_seconds <= 0:
        return False
    try:
        lat1, lon1 = map(float, loc1.split(','))
        lat2, lon2 = map(float, loc2.split(','))

        # Haversine radius parameters
        R = 6371.0
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(
            dlon / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance_km = R * c

        implied_speed = distance_km / (time_window_seconds / 3600.0)
        return implied_speed > 1000.0
    except Exception:
        return False


async def check_deterministic_rules(transaction: dict, redis_client: aioredis.Redis) -> tuple[bool, list[str]]:
    """Evaluates high-speed tripwires using Redis storage vectors"""
    reasons = []
    is_anomaly = False

    amount_raw = float(transaction.get("amount", 0.0))
    currency = transaction.get("currency", "USD").upper()
    sender_id = transaction.get("sender_id")
    timestamp_str = transaction.get("timestamp_iso", "")
    location = transaction.get("location", "")

    exchange_rates = {"USD": 1.0, "EUR": 1.08, "GBP": 1.25, "JPY": 0.0065, "CAD": 0.73, "AUD": 0.65}
    amount_usd = amount_raw * exchange_rates.get(currency, 1.0)

    try:
        clean_ts = timestamp_str.replace("Z", "+00:00")
        tx_time = datetime.fromisoformat(clean_ts)
        tx_hour = tx_time.hour
        tx_epoch = tx_time.timestamp()
    except (ValueError, AttributeError):
        tx_hour = 12
        tx_epoch = datetime.utcnow().timestamp()

    # RULE 1: Smurfing Check
    if 9900 <= amount_usd <= 9999:
        is_anomaly = True
        reasons.append(
            f"Smurfing Triggered: ${amount_usd:.2f} USD bounds look designed to evade financial reporting ceilings.")

    # RULE 2: Suspicious Window Drain Check
    if amount_usd >= 20000 and (tx_hour <= 4 or tx_hour >= 23):
        is_anomaly = True
        reasons.append(
            f"Account Takeover Window: Transfer of ${amount_usd:.2f} USD executed during high-risk dark hours ({tx_hour}:00).")

    # RULE 3: Location Operations & Advanced Geo-Impossibility
    try:
        if location and "," in location:
            lat, lon = map(float, location.split(","))

            if not is_safe_location(lat, lon):
                is_anomaly = True
                reasons.append(
                    f"Geofence Tripwire: Target coordinates ({location}) fall outside known user vectors (US/IT).")

            if sender_id:
                geo_key = f"geo_history:{sender_id}"
                historical_cache = await redis_client.get(geo_key)

                if historical_cache:
                    cached_loc, cached_epoch = historical_cache.split("|")
                    elapsed_seconds = tx_epoch - float(cached_epoch)

                    if calculate_speed_violation(cached_loc, location, elapsed_seconds):
                        is_anomaly = True
                        reasons.append(
                            f"Velocity Vector Violation: Impossibility leap observed from location ({cached_loc}) within {elapsed_seconds:.0f}s window.")

                # Update current trajectory details in Redis cache storage (24-hour retention window)
                await redis_client.setex(geo_key, 86400, f"{location}|{tx_epoch}")
    except Exception as e:
        logger.warning(f"Unable to parse geo tracking properties: {e}")

    # RULE 4: High-Frequency Hit Counter
    if sender_id:
        v_key = f"hit_velocity:{sender_id}"
        total_hits = await redis_client.incr(v_key)
        if total_hits == 1:
            await redis_client.expire(v_key, 60)

        if total_hits > 4:
            is_anomaly = True
            reasons.append(f"Burst Velocity: Sender {sender_id} triggered {total_hits} attempts within 60s window.")

    return is_anomaly, reasons


async def process_message(message, producer: AIOKafkaProducer, redis_client: aioredis.Redis):
    transaction = message.value
    tx_id = transaction.get("transaction_id", "UNKNOWN")
    amount = float(transaction.get("amount", 0.0))
    location = transaction.get("location", "")

    # Increment metric total counter on message receipt
    APP_TRANSACTIONS_CNT_TOTAL_ADC_IN.inc()

    try:
        # 1. High-Speed Structural Check (System 1 Deterministic)
        is_deterministic_anomaly, reasons = await check_deterministic_rules(transaction, redis_client)

        if is_deterministic_anomaly:
            # Rule Tripped: Route straight to the expensive Agent layer for investigation
            logger.warning(f"Deterministic Rule Tripped [TX: {tx_id}] -> Escalating Straight to Agents. Reason: {reasons}")
            transaction["system_1_reasons"] = reasons
            transaction["system_1_routing"] = "DETERMINISTIC_ESCALATION"
            await producer.send_and_wait(OUT_ANOMALY_TOPIC, transaction)
            APP_TRANSACTIONS_CNT_FRAUD_ADC_OUT.inc()
            return

        # 2. Advanced Probabilistic Evaluation (System 1 Machine Learning Layer)
        scores = ml_engine.evaluate_transaction_risk(amount, location)
        risk_score = scores["routing_score"]

        if scores["shadow_active"]:
            logger.info(
                f"Shadow Audit [TX: {tx_id}] -> Champion: {scores['champion_score']:.2f} | Challenger: {scores['challenger_score']:.2f}")

            if "history" not in transaction or not isinstance(transaction["history"], dict):
                transaction["history"] = {}
            transaction["history"]["shadow_metrics"] = {
                "champion_score": scores["champion_score"],
                "challenger_score": scores["challenger_score"]
            }

        # Inject risk parameters for downstream visibility
        transaction["system_1_ml_score"] = risk_score

        # SYSTEM 1 AUTOMATION GATES (Cost-Saving Sieve Configuration)
        if risk_score >= ML_AUTO_DENY_THRESHOLD:
            # AUTO-DENY: Clean operational intercept, skipping System 2 completely
            logger.error(f"[Sieve Gate - AUTO-DENY] TX: {tx_id} | ML Score: {risk_score:.2f} >= {ML_AUTO_DENY_THRESHOLD}. Dropping from Agent Queue.")
            transaction["system_1_reasons"] = [f"ML Automated Intercept: High probability fraud score ({risk_score:.2f})."]
            transaction["system_1_routing"] = "ML_AUTO_DENIED"
            await producer.send_and_wait(OUT_FINAL_TOPIC, transaction)
            APP_TRANSACTIONS_CNT_FRAUD_ADC_OUT.inc()

        elif risk_score <= ML_AUTO_APPROVE_THRESHOLD:
            # AUTO-APPROVE: Verified safe throughput, skipping System 2 completely
            logger.info(f"[Sieve Gate - AUTO-APPROVE] TX: {tx_id} | ML Score: {risk_score:.2f} <= {ML_AUTO_APPROVE_THRESHOLD}. Passing to Settlement.")
            transaction["system_1_routing"] = "ML_AUTO_APPROVED"
            await producer.send_and_wait(OUT_SAFE_TOPIC, transaction)
            APP_TRANSACTIONS_CNT_CLEAN_ADC_OUT.inc()

        else:
            # GREY ZONE: XGBoost is unsure. Escalating to the LLM Agent for tool analysis
            logger.warning(f"🔍 [Sieve Gate - AGENT TRIAGE REQUIRED] TX: {tx_id} | ML Score: {risk_score:.2f} falls inside Grey Zone. Engaging System 2 Agent.")
            transaction["system_1_reasons"] = [f"ML Ambiguity Escalation: Risk score ({risk_score:.2f}) falls in Grey Zone ($0.15 - $0.80$)."]
            transaction["system_1_routing"] = "ML_GREY_ZONE_ESCALATION"
            await producer.send_and_wait(OUT_ANOMALY_TOPIC, transaction)
            APP_TRANSACTIONS_CNT_FRAUD_ADC_OUT.inc()

    except Exception as e:
        logger.error(f"Failed to process transaction cycle for {tx_id}: {e}", exc_info=True)


async def monitor_and_reload_models(inference_engine: FraudMLInference, interval_seconds: int = 10):
    """Background loop that watches model paths for updates and triggers an engine reload"""
    champion_path = "/app/src/ml_service/models/fraud_model.pkl"
    challenger_path = "/app/src/ml_service/models/challenger_model.pkl"

    last_champ_mtime = os.path.getmtime(champion_path) if os.path.exists(champion_path) else 0
    last_chal_mtime = os.path.getmtime(challenger_path) if os.path.exists(challenger_path) else 0

    logger.info("Mtime File Watcher active for model hot-swapping...")

    while True:
        await asyncio.sleep(interval_seconds)
        try:
            # Check Champion update criteria
            if os.path.exists(champion_path):
                current_champ_mtime = os.path.getmtime(champion_path)
                if current_champ_mtime > last_champ_mtime:
                    logger.warning("New Champion model detected on disk! Hot-reloading memory handles...")
                    inference_engine.reload_models()
                    last_champ_mtime = current_champ_mtime

            # Check Challenger update criteria
            if os.path.exists(challenger_path):
                current_chal_mtime = os.path.getmtime(challenger_path)
                if current_chal_mtime > last_chal_mtime:
                    logger.warning("New Challenger model detected on disk! Hot-reloading memory handles...")
                    inference_engine.reload_models()
                    last_chal_mtime = current_chal_mtime
        except Exception as e:
            logger.error(f"Model file watch task encountered an exception: {e}")


async def main():
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

    asyncio.create_task(monitor_and_reload_models(ml_engine, interval_seconds=10))

    logger.info(f"System 1 Pipeline Operating (Optimized Sieve Gate Automation) Listening on: {IN_TOPIC}")

    try:
        async for message in consumer:
            asyncio.create_task(process_message(message, producer, redis_client))
    finally:
        await consumer.stop()
        await producer.stop()
        await redis_client.close()


if __name__ == "__main__":
    # 📊 Expose metrics listener server on port 8002 right on bootstrap initialization
    start_metrics_server(8002)
    asyncio.run(main())