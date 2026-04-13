"""Validator service."""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import redis
import json
import yaml
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)


def validate_transaction(transaction, min_amount, max_amount):
    amount = transaction["transaction"]["public"]["amount"]
    if amount <= min_amount:
        return False, "Amount too low"
    if amount >= max_amount:
        return False, "Amount too high"
    return True, None


def main():
    config = load_config()
    r = redis.Redis(
        host=config["redis"]["host"],
        port=config["redis"]["port"],
        decode_responses=True,
    )

    queue_name = config["redis"]["transaction_queue"]
    correct_key = config["redis"]["correct_transactions_key"]
    invalid_key = config["redis"]["invalid_transactions_key"]
    invalid_list = config["redis"]["invalid_transactions_list"]
    update_channel = config["redis"]["update_channel"]

    min_amount = config["validator"]["min_amount"]
    max_amount = config["validator"]["max_amount"]

    logging.info("Validator starting.")

    while True:
        _, transaction_json = r.brpop(queue_name)
        transaction = json.loads(transaction_json)

        is_valid, reason = validate_transaction(transaction, min_amount, max_amount)

        if is_valid:
            r.incr(correct_key)
            logging.info(
                f"Validated (Correct): {transaction['transaction']['public']['transaction_id']}"
            )
        else:
            r.incr(invalid_key)
            # Add the reason to the transaction object itself before storing it
            transaction["reason"] = reason
            r.lpush(invalid_list, json.dumps(transaction))
            logging.warning(
                f"Validated (Invalid): {transaction['transaction']['public']['transaction_id']} - Reason: {reason}"
            )

        r.publish(update_channel, "update")


if __name__ == "__main__":
    main()
