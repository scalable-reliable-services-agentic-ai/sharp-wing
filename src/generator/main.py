from src.generator.transaction import Transaction

import redis
import random
import time
import yaml
import logging
from jinja2 import Environment, FileSystemLoader

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)


def main():
    config = load_config()
    r = redis.Redis(
        host=config["redis"]["host"],
        port=config["redis"]["port"],
        decode_responses=True,
    )

    # Jinja2 setup
    env = Environment(loader=FileSystemLoader("src/generator/templates"))
    template = env.get_template("transaction_template.json.j2")

    # Redis keys
    queue_name = config["redis"]["transaction_queue"]
    tps_key = config["redis"]["generator_tps_key"]
    invalid_perc_key = config["redis"]["generator_invalid_perc_key"]

    logging.info("Generator starting.")
    while True:
        # Get current settings from Redis, with fallbacks to config file
        try:
            target_tps = float(r.get(tps_key) or config["generator"]["default_tps"])
            invalid_percentage = int(
                r.get(invalid_perc_key)
                or config["generator"]["default_invalid_percentage"]
            )
        except (ValueError, TypeError):
            logging.warning("Could not parse settings from Redis, using defaults.")
            target_tps = config["generator"]["default_tps"]
            invalid_percentage = config["generator"]["default_invalid_percentage"]

        # Calculate sleep time based on target TPS
        avg_sleep = 1.0 / target_tps
        min_sleep = avg_sleep * 0.8
        max_sleep = avg_sleep * 1.2

        # Generate a new transaction
        t = Transaction()
        transaction_data = t.generate_transaction_data()

        # Decide if this transaction should be invalid
        if random.uniform(0, 100) < invalid_percentage:
            # Invalidate the amount
            transaction_data["amount"] = (
                transaction_data["amount"] * -100
            )  # A simple way to make it invalid
            logging.warning(
                f"Generated an invalid transaction ({transaction_data['transaction_id']})"
            )

        # Render the template and push to Redis
        transaction_json_str = template.render(transaction_data)
        r.lpush(queue_name, transaction_json_str)
        logging.info(f"Produced: {transaction_data['transaction_id']}")

        # Sleep for a bit
        time.sleep(random.uniform(min_sleep, max_sleep))


if __name__ == "__main__":
    main()
