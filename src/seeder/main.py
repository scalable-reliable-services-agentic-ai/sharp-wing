import os
import json
import psycopg2
from psycopg2.extras import execute_values
import random
from datetime import datetime, timedelta
from faker import Faker
from src.config import settings, configure_logging
from src.generator.transaction import Transaction

logger = configure_logging(__name__)


fake = Faker()

# PostgreSQL connection configuration
DB_CONFIG = {
    "dbname": settings.postgres_db,
    "user": settings.postgres_user,
    "password": settings.postgres_password,
    "host": settings.db_host,
    "port": settings.postgres_port,
}


def setup_db():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        with open(os.path.join(current_dir, "sql/setup.sql"), "r") as f:
            cursor.execute(f.read())
    conn.commit()
    return conn


def get_approx_geolocation(region):
    if region == "IT":
        return f"{random.uniform(36.5, 47.0)}, {random.uniform(6.5, 18.5)}"
    elif region == "SE_ASIA":
        return f"{random.uniform(-10.0, 20.0)}, {random.uniform(95.0, 140.0)}"
    elif region == "US":
        return f"{random.uniform(25.0, 49.0)}, {random.uniform(-125.0, -66.0)}"
    else:
        return f"{random.uniform(-90, 90)}, {random.uniform(-180, 180)}"


def generate_client_data(num_clients):
    clients = []
    persona_types = [
        "Standard",
        "VIP",
        "Corporate",
        "NightOwl",
        "Fraud_StolenCard",
        "Fraud_Smurfing",
        "Fraud_ATO",
        "Fraud_ImpossibleTravel",
    ]
    weights = [0.35, 0.10, 0.15, 0.20, 0.05, 0.05, 0.05, 0.05]

    for _ in range(num_clients):
        ctype = random.choices(persona_types, weights=weights, k=1)[0]
        clients.append(
            {"client_id": random.randint(100_000, 999_999), "client_type": ctype}
        )
    return clients


def apply_amount_and_geoloc(t, ctype, base_time, day_code, unique_id_counter):
    """Router function to apply specific persona logic"""
    t.client_type = ctype

    if ctype == "Standard":
        return _set_standard_amount_and_geoloc(t, base_time)
    elif ctype == "VIP":
        return _set_vip_amount_and_geoloc(t, base_time)
    elif ctype == "Corporate":
        return _set_corporate_amount_and_geoloc(t, base_time)
    elif ctype == "NightOwl":
        return _set_nightowl_amount_and_geoloc(t, base_time)
    elif ctype == "Fraud_StolenCard":
        return _set_stolen_card_amount_and_geoloc(t, base_time)
    elif ctype == "Fraud_Smurfing":
        return _set_smurfing_amount_and_geoloc(t, base_time)
    elif ctype == "Fraud_ATO":
        return _set_ato_amount_and_geoloc(t, base_time)
    elif ctype == "Fraud_ImpossibleTravel":
        return _set_impossible_travel_amount_and_geoloc(
            t, base_time, day_code, unique_id_counter
        )


def _set_standard_amount_and_geoloc(t, base_time):
    t.amount = round(random.uniform(5.0, 150.0), 2)
    t.geolocation = get_approx_geolocation("IT")
    t.timestamp = base_time + timedelta(
        days=random.randint(0, 30), hours=random.randint(7, 21)
    )
    t.is_fraud = False
    t.fraud_reason = "None"
    return [t]


def _set_vip_amount_and_geoloc(t, base_time):
    t.amount = round(random.uniform(1000.0, 25000.0), 2)
    t.geolocation = get_approx_geolocation(random.choice(["IT", "US"]))
    t.timestamp = base_time + timedelta(
        days=random.randint(0, 30), hours=random.randint(9, 20)
    )
    t.is_fraud = False
    t.fraud_reason = "None"
    return [t]


def _set_corporate_amount_and_geoloc(t, base_time):
    t.amount = round(random.uniform(10000.0, 100000.0), 2)
    t.geolocation = get_approx_geolocation("IT")
    t.timestamp = base_time + timedelta(
        days=random.randint(0, 30), hours=random.randint(9, 17)
    )
    t.is_fraud = False
    t.fraud_reason = "None"
    return [t]


def _set_nightowl_amount_and_geoloc(t, base_time):
    t.amount = round(random.uniform(1.0, 30.0), 2)
    t.geolocation = get_approx_geolocation("IT")
    t.timestamp = base_time + timedelta(
        days=random.randint(0, 30), hours=random.choice([23, 0, 1, 2, 3, 4])
    )
    t.is_fraud = False
    t.fraud_reason = "None"
    return [t]


def _set_stolen_card_amount_and_geoloc(t, base_time):
    t.amount = round(random.uniform(500.0, 2000.0), 2)
    t.geolocation = get_approx_geolocation("GLOBAL")
    t.timestamp = base_time + timedelta(
        days=random.randint(0, 30), hours=random.randint(0, 23)
    )
    t.is_fraud = True
    t.fraud_reason = "Stolen Card Pattern"
    return [t]


def _set_smurfing_amount_and_geoloc(t, base_time):
    t.amount = round(random.uniform(9900.0, 9999.0), 2)
    t.geolocation = get_approx_geolocation("GLOBAL")
    t.timestamp = base_time + timedelta(
        days=random.randint(0, 30), hours=random.randint(9, 17)
    )
    t.is_fraud = True
    t.fraud_reason = "Smurfing Pattern"
    return [t]


def _set_ato_amount_and_geoloc(t, base_time):
    t.amount = round(random.uniform(20000.0, 50000.0), 2)
    t.geolocation = get_approx_geolocation("GLOBAL")
    t.timestamp = base_time + timedelta(
        days=random.randint(0, 30), hours=random.choice([2, 3, 4])
    )
    t.is_fraud = True
    t.fraud_reason = "Account Takeover (ATO)"
    return [t]


def _set_impossible_travel_amount_and_geoloc(t, base_time, day_code, unique_id_counter):
    # Transaction 1: Normal IT transaction
    t.timestamp = base_time + timedelta(days=random.randint(0, 30), hours=10)
    t.amount = round(random.uniform(10.0, 50.0), 2)
    t.geolocation = get_approx_geolocation("IT")
    t.is_fraud = False
    t.fraud_reason = "None"

    # Transaction 2: Impossible travel to SE_ASIA 30 mins later
    t2 = Transaction()
    t2.sender_id = t.sender_id
    t2.transaction_id = (day_code * 1_000_000) + unique_id_counter
    t2.timestamp = t.timestamp + timedelta(minutes=30)
    t2.amount = round(random.uniform(5000.0, 15000.0), 2)
    t2.geolocation = get_approx_geolocation("SE_ASIA")
    t2.is_fraud = True
    t2.fraud_reason = "Impossible Travel"
    t2.client_type = t.client_type

    return [t, t2]


def generate_transactions(clients, target_rows):
    transactions_data = []
    base_time = datetime.now() - timedelta(days=30)
    unique_id_counter = 100_000
    day_code = int(datetime.now().strftime("%Y%m%d"))

    while len(transactions_data) < target_rows:
        client = random.choice(clients)
        ctype = client["client_type"]

        # Base Setup
        t = Transaction()
        t.sender_id = client["client_id"]
        t.transaction_id = (day_code * 1_000_000) + unique_id_counter
        unique_id_counter += 1

        # Apply the logic
        configured_txs = apply_amount_and_geoloc(
            t, ctype, base_time, day_code, unique_id_counter
        )

        # Handle IDs for double-transactions
        if len(configured_txs) > 1:
            unique_id_counter += 1  # Bump counter again since t2 consumed an ID

        # Append finalized data
        for tx in configured_txs:
            data = tx.generate_transaction_data()
            transactions_data.append(
                (data, tx.is_fraud, tx.client_type, tx.fraud_reason)
            )

    return transactions_data


def load_to_db(conn, transactions_data):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    with conn.cursor() as cursor:
        formatted_rows = []
        for data, is_fraud, ctype, fraud_reason in transactions_data:
            formatted_rows.append(
                (
                    data["transaction_id"],
                    data["client_id"],
                    data["receiver_id"],
                    data["timestamp_iso"],
                    data["transaction_type"],
                    data["channel"],
                    data["amount"],
                    data["currency"],
                    data["location"],
                    data["ip_address"],
                    data["mac_address"],
                    data["fingerprint"],
                    data["session_id"],
                    data["timestamp_ms"],
                    "PENDING",
                    json.dumps([]),
                    ctype,
                    is_fraud,
                    fraud_reason,
                )
            )

        with open(os.path.join(current_dir, "sql/insert_transaction.sql"), "r") as f:
            insert_query = f.read()
        execute_values(cursor, insert_query, formatted_rows)
        conn.commit()
        logger.info(
            f"Successfully inserted {len(formatted_rows)} rows into PostgreSQL."
        )


if __name__ == "__main__":
    logger.info("Connecting to PostgreSQL...")
    with setup_db() as conn:
        logger.info("Generating clients...")
        clients = generate_client_data(num_clients=5000)

        logger.info("Generating transactions based on personas...")
        transactions_data = generate_transactions(clients, target_rows=15000)

        logger.info("Loading data into Database...")
        load_to_db(conn, transactions_data)

    logger.info("Database seeding complete!")
