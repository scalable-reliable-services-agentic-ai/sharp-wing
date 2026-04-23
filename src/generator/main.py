from src.generator.transaction import Transaction

import redis
import psycopg2
import random
import time
import yaml
import logging
from jinja2 import Environment, FileSystemLoader

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)


def fetch_existing_clients(config):
    """Fetches a pool of existing clients from PostgreSQL on startup"""
    try:
        logging.info("Connecting to PostgreSQL to load existing clients...")
        conn = psycopg2.connect(
            host=config["postgres"]["host"],
            port=config["postgres"]["port"],
            dbname=config["postgres"]["dbname"],
            user=config["postgres"]["user"],
            password=config["postgres"]["password"]
        )
        cursor = conn.cursor()
        # Fetch up to 5000 distinct clients
        cursor.execute("SELECT DISTINCT sender_id, client_type FROM transactions LIMIT 5000;")
        clients = [{"client_id": row[0], "client_type": row[1]} for row in cursor.fetchall()]
        conn.close()
        logging.info(f"Successfully loaded {len(clients)} existing clients.")
        return clients
    except Exception as e:
        logging.error(f"Failed to fetch clients from PostgreSQL: {e}")
        return []


def apply_persona(t, client_type):
    """Applies behavior patterns and explicit fraud labeling to a transaction"""
    t.client_type = client_type

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

    return t


def main():
    config = load_config()
    r = redis.Redis(host=config["redis"]["host"], port=config["redis"]["port"], decode_responses=True)

    env = Environment(loader=FileSystemLoader("src/generator/templates"))
    template = env.get_template("transaction_template.json.j2")

    queue_name = config["redis"]["transaction_queue"]
    tps_key = config["redis"]["generator_tps_key"]
    invalid_perc_key = config["redis"]["generator_invalid_perc_key"]

    # Load existing clients from the database once on startup
    existing_clients = fetch_existing_clients(config)

    logging.info("Generator starting.")
    while True:
        try:
            target_tps = float(r.get(tps_key) or config["generator"]["default_tps"])
            invalid_percentage = int(r.get(invalid_perc_key) or config["generator"]["default_invalid_percentage"])
        except (ValueError, TypeError):
            target_tps = config["generator"]["default_tps"]
            invalid_percentage = config["generator"]["default_invalid_percentage"]

        avg_sleep = 1.0 / target_tps

        # Decide: Use existing client (80%) or generate a new one (20%)
        if existing_clients and random.random() < 0.8:
            client = random.choice(existing_clients)
            ctype = client["client_type"]
            cid = client["client_id"]
        else:
            persona_types = ["Standard", "VIP", "Corporate", "NightOwl", "Fraud_StolenCard", "Fraud_Smurfing",
                             "Fraud_ATO", "Fraud_ImpossibleTravel"]
            weights = [0.35, 0.10, 0.15, 0.20, 0.05, 0.05, 0.05, 0.05]
            ctype = random.choices(persona_types, weights=weights, k=1)[0]
            cid = random.randint(100_000, 999_999)

        # Create transaction and apply the persona and fraud labels
        t = Transaction()
        t.sender_id = cid
        t = apply_persona(t, ctype)
        transaction_data = t.generate_transaction_data()

        # Old validation logic (randomly breaking fields to test schema validation)
        if random.uniform(0, 100) < invalid_percentage:
            if random.random() < 0.5:
                transaction_data["amount"] = transaction_data["amount"] * -100
                transaction_data["is_fraud"] = True
                transaction_data["fraud_reason"] = "Schema Validation Failed: Negative Amount"
            else:
                field = random.choice(["sender_id", "receiver_id", "ip_address"])
                transaction_data[field] = ""
                transaction_data["is_fraud"] = True
                transaction_data["fraud_reason"] = f"Schema Validation Failed: Missing {field}"

        transaction_json_str = template.render(transaction_data)
        r.lpush(queue_name, transaction_json_str)

        if transaction_data.get("is_fraud"):
            logging.warning(
                f"Produced FRAUD ({transaction_data['transaction_id']}): {transaction_data['fraud_reason']}")
        else:
            logging.info(f"Produced OK ({transaction_data['transaction_id']})")

        time.sleep(random.uniform(avg_sleep * 0.8, avg_sleep * 1.2))


if __name__ == "__main__":
    main()