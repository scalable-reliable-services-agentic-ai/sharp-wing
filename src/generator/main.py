import json
import random
import asyncio
import psycopg2
from aiokafka import AIOKafkaProducer
from src.config import settings, configure_logging
from src.generator.transaction import Transaction
import time
import math

logger = configure_logging(__name__)

# Environment and Constants
KAFKA_BROKER = settings.kafka_broker
KAFKA_TOPIC = settings.kafka_raw_transactions_topic


def fetch_existing_clients():
    """Fetches a pool of existing clients from PostgreSQL/TimescaleDB on startup"""
    try:
        logger.info("Connecting to Database to load existing clients...")
        conn = psycopg2.connect(
            host=settings.db_host,
            port=getattr(settings, "postgres_port", 5432),
            dbname=getattr(settings, "postgres_db", "fraud_detection_db"),
            user=getattr(settings, "postgres_user", "postgres"),
            password=getattr(settings, "postgres_password", "password"),
        )
        cursor = conn.cursor()

        cursor.execute(
            "SELECT DISTINCT sender_id, client_type FROM transactions LIMIT 5000;"
        )
        clients = [
            {"sender_id": row[0], "client_type": row[1]} for row in cursor.fetchall()
        ]
        conn.close()
        logger.info(f"Successfully loaded {len(clients)} existing clients.")
        return clients
    except Exception as e:
        logger.error(f"Failed to fetch clients from Database: {e}", exc_info=True)
        return []


def generate_realistic_location(client_type):
    """Generates coordinates based on an 80/20 safe-zone distribution with diverse high-risk regions"""

    # 1. Diverse High-Risk Zones for Fraud Personas
    if client_type in ["Fraud_StolenCard", "Fraud_ImpossibleTravel"]:
        # Randomly pick a bounding box from various global regions
        high_risk_zones = [
            f"{random.uniform(-10.0, 20.0)}, {random.uniform(95.0, 140.0)}",  # SE Asia
            f"{random.uniform(-35.0, 35.0)}, {random.uniform(-17.0, 51.0)}",  # Africa
            f"{random.uniform(40.0, 60.0)}, {random.uniform(20.0, 50.0)}",  # Eastern Europe
            f"{random.uniform(-55.0, 12.0)}, {random.uniform(-80.0, -35.0)}",  # South America
        ]
        return random.choice(high_risk_zones)

    # 2. 80% chance for normal personas to be in Safe Zones
    if random.random() < 0.80:
        # Split 50/50 between Italy and US
        if random.random() < 0.50:
            return f"{random.uniform(36.5, 47.0)}, {random.uniform(6.5, 18.5)}"  # Italy Box
        else:
            return f"{random.uniform(25.0, 49.0)}, {random.uniform(-125.0, -66.0)}"  # US Box

    # 3. 20% chance for normal personas to be traveling (creates healthy false positives for AI)
    return f"{random.uniform(-90.0, 90.0)}, {random.uniform(-180.0, 180.0)}"


def apply_persona(t: Transaction, client_type):
    """Applies behavior patterns, realistic locations, and explicit fraud labeling"""
    t.client_type = client_type

    # Apply the realistic location override
    t.geolocation = generate_realistic_location(client_type)
    t.location = t.geolocation  # Ensure both properties match just in case

    # Legitimate personas
    if client_type == "Standard":
        t.amount = round(random.uniform(5.0, 150.0), 2)
    elif client_type == "VIP":
        t.amount = round(random.uniform(1000.0, 25000.0), 2)
    elif client_type == "Corporate":
        t.amount = round(random.uniform(10000.0, 100000.0), 2)
    elif client_type == "NightOwl":
        t.amount = round(random.uniform(1.0, 30.0), 2)

    # Fraudulent personas (explicitly labeled)
    elif client_type == "Fraud_StolenCard":
        t.amount = round(random.uniform(500.0, 2000.0), 2)
        t.is_fraud = True
        t.fraud_reason = "Stolen Card Pattern: High burst amount from unknown device"

    elif client_type == "Fraud_Smurfing":
        t.amount = round(random.uniform(9900.0, 9999.0), 2)
        t.is_fraud = True
        t.fraud_reason = "Smurfing Pattern: Amount deliberately placed just below 10k reporting limit"

    elif client_type == "Fraud_ATO":
        t.amount = round(random.uniform(20000.0, 50000.0), 2)
        t.is_fraud = True
        t.fraud_reason = "Account Takeover (ATO): Massive wallet drain to new recipient"

    elif client_type == "Fraud_ImpossibleTravel":
        t.amount = round(random.uniform(5000.0, 15000.0), 2)
        t.is_fraud = True
        t.fraud_reason = "Impossible Travel: Live geolocation conflicts with recent historical location"

    if random.random() < 0.025:
        t.invalidate_transaction_amount()

    return t


# Connection Handlers
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
            logger.error(f"Could not connect to Kafka: {e}. Retrying...", exc_info=True)
            await asyncio.sleep(5)


def generation_time_from_model():
    min_sleep = settings.generator_min_sleep_ms / 1000.0    # peak activity, high frequency
    max_sleep = settings.generator_max_sleep_ms / 1000.0    #  low activity,  low frequency
    CYCLE_LENGTH_SECONDS = settings.generator_cycle_length_seconds
    return sinusoidal_sleep_time(min_sleep, max_sleep, CYCLE_LENGTH_SECONDS)


def sinusoidal_sleep_time(min_sleep, max_sleep, cycle_lenght_seconds, noise_factor=0.1):
    current_time = time.time()
    
    sine_value = math.sin((2 * math.pi * current_time) / cycle_lenght_seconds)
    normalized_sine = (sine_value + 1) / 2
    
    # inversed sine -> peak of sine, min sleep; trough of sine, max sleep
    target_sleep = min_sleep + (1.0 - normalized_sine) * (max_sleep - min_sleep)
    
    # random noise and ensuring a minimum sleep time
    actual_sleep = random.uniform(target_sleep * (1.0-noise_factor), target_sleep * (1.0+noise_factor))
    actual_sleep = max(1e-8, actual_sleep)
    return actual_sleep


# Main Application
async def main():
    # Load existing clients from the database once on startup
    existing_clients = fetch_existing_clients()

    producer = await get_kafka_producer()
    logger.info(f"Generator starting, producing to topic '{KAFKA_TOPIC}'.")
    
    # Zmienne do kontrolowania przepustowości
    tasks = set()
    MAX_CONCURRENT_MESSAGES = 1000

    try:
        while True:
            try:
                # Decide: Use existing client (80%) or generate a new one (20%)
                if existing_clients and random.random() < 0.8:
                    client = random.choice(existing_clients)
                    ctype = client["client_type"]
                    cid = client["sender_id"]
                else:
                    persona_types = ["Standard", "VIP", "Corporate", "NightOwl", "Fraud_StolenCard", "Fraud_Smurfing", "Fraud_ATO", "Fraud_ImpossibleTravel"]
                    weights = [0.35, 0.10, 0.15, 0.20, 0.05, 0.05, 0.05, 0.05]
                    ctype = random.choices(persona_types, weights=weights, k=1)[0]
                    cid = random.randint(100_000, 999_999)

                # Generate transaction and apply persona
                transaction = Transaction()
                transaction.sender_id = cid
                transaction = apply_persona(transaction, ctype)

                transaction_data = (
                    transaction.get_kafka_message()
                    if hasattr(transaction, "get_kafka_message")
                    else transaction.generate_transaction_data()
                )

                # ZMIANA 1: Używamy 'send' zamiast 'send_and_wait'. 
                # 'send' zwraca Future, który dodajemy do zbioru tasków.
                task = asyncio.create_task(producer.send(KAFKA_TOPIC, transaction_data))
                tasks.add(task)
                task.add_done_callback(tasks.discard) # Usuwa zadanie po jego zakończeniu

                # ZMIANA 2: Ograniczenie logowania w trakcie szczytu
                sleep_time = generation_time_from_model()
                
                if getattr(transaction, "is_fraud", False) or transaction_data.get("is_fraud"):
                    logger.warning(f"Produced FRAUD ({transaction_data['transaction_id']}): {transaction_data.get('fraud_reason', 'Unknown')}")
                # Logujemy OK tylko jeśli nie generujemy zbyt szybko (np. sleep > 0.01), 
                # aby nie zablokować konsoli tysiącami logów na sekundę.
                elif sleep_time > 0.01: 
                    logger.info(f"Produced OK ({transaction_data['transaction_id']})")

                # ZMIANA 3: Jeśli mamy za dużo niepotwierdzonych wiadomości (np. 1000), 
                # czekamy aż Kafka trochę ich przetworzy, żeby nie zapchać pamięci.
                if len(tasks) >= MAX_CONCURRENT_MESSAGES:
                    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    tasks = pending

                # ZMIANA 4: Jeśli wyliczony czas jest bardzo mały, 
                # omijamy asyncio.sleep(0), żeby uniknąć narzutu pętli zdarzeń.
                if sleep_time > 0.001: 
                    await asyncio.sleep(sleep_time)

            except Exception as e:
                logger.error(f"An error occurred in the main loop: {e}", exc_info=True)
                await asyncio.sleep(5)
    finally:
        # Zanim zamkniemy, upewnijmy się, że wszystkie wiadomości zostały wysłane
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())