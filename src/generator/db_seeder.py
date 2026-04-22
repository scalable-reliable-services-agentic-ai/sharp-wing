import os
import sys

# This dynamically finds the project root and adds it to Python's path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../")) # Adjusts if saved in src/generator
sys.path.insert(0, current_dir)
sys.path.insert(0, project_root)

import psycopg2
from psycopg2.extras import execute_values
import random
from datetime import datetime, timedelta
from faker import Faker
from src.generator.transaction import Transaction

fake = Faker()

# PostgreSQL connection configuration
# we have to update these with our actual Docker or Local Postgres credentials
DB_CONFIG = {
    "dbname": "fraud_detection_db",
    "user": "postgres",
    "password": "password",
    "host": "localhost",
    "port": "5432"
}


def setup_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute('''
                   CREATE TABLE IF NOT EXISTS transactions
                   (
                       transaction_id
                       BIGINT
                       PRIMARY
                       KEY,
                       sender_id
                       BIGINT,
                       receiver_id
                       BIGINT,
                       timestamp
                       TIMESTAMP,
                       transaction_type
                       VARCHAR
                   (
                       50
                   ),
                       channel VARCHAR
                   (
                       50
                   ),
                       amount DOUBLE PRECISION,
                       currency VARCHAR
                   (
                       10
                   ),
                       status VARCHAR
                   (
                       50
                   ),
                       geolocation VARCHAR
                   (
                       100
                   ),
                       ip_address VARCHAR
                   (
                       50
                   ),
                       mac_address VARCHAR
                   (
                       50
                   ),
                       fingerprint VARCHAR
                   (
                       100
                   ),
                       session_id VARCHAR
                   (
                       100
                   ),
                       is_flagged_fraud BOOLEAN,
                       client_type VARCHAR
                   (
                       50
                   )
                       )
                   ''')
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
        "Standard", "VIP", "Corporate", "NightOwl",
        "Fraud_StolenCard", "Fraud_Smurfing", "Fraud_ATO", "Fraud_ImpossibleTravel"
    ]
    weights = [0.35, 0.10, 0.15, 0.20, 0.05, 0.05, 0.05, 0.05]

    for _ in range(num_clients):
        ctype = random.choices(persona_types, weights=weights, k=1)[0]
        clients.append({
            "client_id": random.randint(100_000, 999_999),
            "client_type": ctype
        })
    return clients


def generate_transactions(clients, target_rows):
    transactions_data = []
    base_time = datetime.now() - timedelta(days=30)

    # a counter to guarantee 100% unique transaction IDs
    unique_id_counter = 100_000

    while len(transactions_data) < target_rows:
        client = random.choice(clients)
        ctype = client["client_type"]
        is_fraud = False

        t = Transaction()
        t.sender_id = client["client_id"]

        # Ensure the ID is totally unique by appending our counter
        day_code = int(datetime.now().strftime("%Y%m%d"))
        t.transaction_id = (day_code * 1_000_000) + unique_id_counter
        unique_id_counter += 1

        if ctype == "Standard":
            t.amount = round(random.uniform(5.0, 150.0), 2)
            t.geolocation = get_approx_geolocation("IT")
            tx_time = base_time + timedelta(days=random.randint(0, 30), hours=random.randint(7, 21))

        elif ctype == "VIP":
            t.amount = round(random.uniform(1000.0, 25000.0), 2)
            t.geolocation = get_approx_geolocation(random.choice(["IT", "US"]))
            tx_time = base_time + timedelta(days=random.randint(0, 30), hours=random.randint(9, 20))

        elif ctype == "Corporate":
            t.amount = round(random.uniform(10000.0, 100000.0), 2)
            t.geolocation = get_approx_geolocation("IT")
            tx_time = base_time + timedelta(days=random.randint(0, 30), hours=random.randint(9, 17))

        elif ctype == "NightOwl":
            t.amount = round(random.uniform(1.0, 30.0), 2)
            t.geolocation = get_approx_geolocation("IT")
            tx_time = base_time + timedelta(days=random.randint(0, 30), hours=random.choice([23, 0, 1, 2, 3, 4]))

        elif ctype == "Fraud_StolenCard":
            t.amount = round(random.uniform(500.0, 2000.0), 2)
            t.geolocation = get_approx_geolocation("GLOBAL")
            tx_time = base_time + timedelta(days=random.randint(0, 30), hours=random.randint(0, 23))
            is_fraud = True

        elif ctype == "Fraud_Smurfing":
            t.amount = round(random.uniform(9900.0, 9999.0), 2)
            t.geolocation = get_approx_geolocation("GLOBAL")
            tx_time = base_time + timedelta(days=random.randint(0, 30), hours=random.randint(9, 17))
            is_fraud = True

        elif ctype == "Fraud_ATO":
            t.amount = round(random.uniform(20000.0, 50000.0), 2)
            t.geolocation = get_approx_geolocation("GLOBAL")
            tx_time = base_time + timedelta(days=random.randint(0, 30), hours=random.choice([2, 3, 4]))
            is_fraud = True

        elif ctype == "Fraud_ImpossibleTravel":
            t1_time = base_time + timedelta(days=random.randint(0, 30), hours=10)
            t.amount = round(random.uniform(10.0, 50.0), 2)
            t.geolocation = get_approx_geolocation("IT")
            t.timestamp = t1_time.isoformat()

            data1 = t.generate_transaction_data()
            transactions_data.append((data1, False, ctype))

            t2 = Transaction()
            t2.sender_id = client["client_id"]

            # OVERRIDE: Ensure the second transaction ID is also totally unique
            t2.transaction_id = (day_code * 1_000_000) + unique_id_counter
            unique_id_counter += 1

            t2_time = t1_time + timedelta(minutes=30)
            t2.amount = round(random.uniform(5000.0, 15000.0), 2)
            t2.geolocation = get_approx_geolocation("SE_ASIA")
            t2.timestamp = t2_time.isoformat()

            data2 = t2.generate_transaction_data()
            transactions_data.append((data2, True, ctype))
            continue

        t.timestamp = tx_time.isoformat()

        data = t.generate_transaction_data()
        transactions_data.append((data, is_fraud, ctype))

    return transactions_data


def load_to_db(conn, transactions_data):
    cursor = conn.cursor()
    formatted_rows = []
    for data, is_fraud, ctype in transactions_data:
        formatted_rows.append((
            data["transaction_id"], data["sender_id"], data["receiver_id"],
            data["timestamp"], data["transaction_type"], data["channel"],
            data["amount"], data["currency"], data["status"],
            data["geolocation"], data["ip_address"], data["mac_address"],
            data["fingerprint"], data["session_id"], is_fraud, ctype
        ))

    # PostgreSQL uses %s instead of ? for parameter substitution
    # execute_values is much faster than executemany for bulk inserts in Postgres
    insert_query = '''
                   INSERT INTO transactions (transaction_id, sender_id, receiver_id, timestamp, transaction_type, \
                                             channel, amount, currency, status, geolocation, ip_address, \
                                             mac_address, fingerprint, session_id, is_flagged_fraud, client_type) \
                   VALUES %s \
                   '''
    execute_values(cursor, insert_query, formatted_rows)
    conn.commit()
    print(f"Successfully inserted {len(formatted_rows)} rows into PostgreSQL.")


if __name__ == "__main__":
    print("Connecting to PostgreSQL...")
    conn = setup_db()

    print("Generating clients...")
    clients = generate_client_data(num_clients=5000)

    print("Generating transactions based on personas...")
    transactions_data = generate_transactions(clients, target_rows=150000)

    print("Loading data into Database...")
    load_to_db(conn, transactions_data)

    conn.close()
    print("Database seeding complete!")
