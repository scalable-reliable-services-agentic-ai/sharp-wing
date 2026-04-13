"""Reporter service."""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from flask import Flask, render_template, jsonify
from flask_sse import sse
import redis
import yaml
import json
from datetime import datetime
import threading

app = Flask(__name__, template_folder="templates", static_folder="static")


def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)


config = load_config()

# Configure SSE to use Redis
app.config["REDIS_URL"] = f"redis://{config['redis']['host']}:{config['redis']['port']}"
app.register_blueprint(sse, url_prefix="/stream")

# Connect to Redis
r = redis.Redis(
    host=config["redis"]["host"], port=config["redis"]["port"], decode_responses=True
)

# App start time
start_time_dt = datetime.now()

# Redis keys from config
correct_key = config["redis"]["correct_transactions_key"]
invalid_key = config["redis"]["invalid_transactions_key"]
invalid_list = config["redis"]["invalid_transactions_list"]
update_channel = config["redis"]["update_channel"]


@app.route("/")
def home():
    """Serves the main dashboard page."""
    return render_template("home.html")


@app.route("/data")
def data():
    """API endpoint to get current statistics."""
    correct_count = int(r.get(correct_key) or 0)
    invalid_count = int(r.get(invalid_key) or 0)
    total_verified = correct_count + invalid_count

    time_elapsed = (datetime.now() - start_time_dt).total_seconds()
    avg_tps = total_verified / time_elapsed if time_elapsed > 0 else 0

    return jsonify(
        start_time=start_time_dt.strftime("%Y-%m-%d %H:%M:%S"),
        current_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        total_verified=total_verified,
        correct_count=correct_count,
        invalid_count=invalid_count,
        avg_tps=round(avg_tps, 2),
    )


@app.route("/invalid")
def invalid():
    """Serves the page for invalid transactions."""
    transactions_json = r.lrange(invalid_list, 0, -1)
    transactions = [json.loads(t) for t in transactions_json]
    return render_template("invalid.html", transactions=transactions)


def listen_for_updates():
    """Listens to Redis Pub/Sub and sends SSE events."""
    pubsub = r.pubsub()
    pubsub.subscribe(update_channel)
    for message in pubsub.listen():
        if message["type"] == "message":
            with app.app_context():
                sse.publish({"status": "updated"}, type="data_update")


if __name__ == "__main__":
    # Run the Redis listener in a background thread
    listener_thread = threading.Thread(target=listen_for_updates, daemon=True)
    listener_thread.start()
    # Start the Flask app
    app.run(host="0.0.0.0", port=5000, threaded=True)
