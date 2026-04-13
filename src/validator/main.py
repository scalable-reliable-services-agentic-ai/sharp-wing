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


def __validate_string_data(transaction_body, attribute):
    if transaction_body[attribute] is None:
        return False
    elif transaction_body[attribute] == "":
        return False
    else:
        return True

def _validate_sender(transaction_body):
    return __validate_string_data(transaction_body, 'sender_id')

def _validate_receiver(transaction_body):
    return __validate_string_data(transaction_body, 'receiver_id')

def _validate_amount(transaction_body):
    return False if transaction_body['amount'] <= 0 else True

def _validate_geolocation(transaction_body):
    return __validate_string_data(transaction_body, 'geolocation')

def _validate_ipaddress(transaction_body):
    # TODO: check string format 
    return __validate_string_data(transaction_body, 'ip_address')

def _validate_macaddress(transaction_body):
    # TODO: check string format
    return __validate_string_data(transaction_body, 'mac_address')

def _validate_fingerprint(transaction_body):
    return __validate_string_data(transaction_body, 'fingerprint')

def _validate_sessionid(transaction_body):
    return __validate_string_data(transaction_body, 'session_id')


def validate_transaction(transaction):
    t_body = transaction['transaction']['public']
    if not _validate_sender(t_body):
        return False, "invalid sender id"
    if not _validate_receiver(t_body):
        return False, "invalid receiver id"
    if not _validate_amount(t_body):
        return False, "invalid amount"
    if not _validate_geolocation(t_body):
        return False, "invalid geolocation"
    if not _validate_ipaddress(t_body):
        return False, "invalid ip address"
    if not _validate_macaddress(t_body):
        return False, "invalid mac address"
    if not _validate_fingerprint(t_body):
        return False, "invalid fingerprint"
    if not _validate_sessionid(t_body):
        return False, "invalid session id"
    return True, ""
    

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

    # min_amount = config["validator"]["min_amount"]
    # max_amount = config["validator"]["max_amount"]

    logging.info("Validator starting.")

    while True:
        _, transaction_json = r.brpop(queue_name)
        transaction = json.loads(transaction_json)

        is_valid, reason = validate_transaction(transaction)

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
