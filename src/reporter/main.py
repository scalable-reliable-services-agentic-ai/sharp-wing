import json
from collections import deque
from datetime import datetime
from flask import Flask, jsonify, render_template
from sqlalchemy import create_engine, func, inspect
from sqlalchemy.orm import sessionmaker
import redis
from src.config import settings
from src.database.models import Transaction

from prometheus_client import Gauge
from src.prometheus_metrics.metrics import start_metrics_server

AVERAGE_TPS = Gauge(
    "average_transactions_per_second",
    "Average number of incoming transactions per second",
)


# --- Environment & DB Setup ---
DATABASE_URL = settings.database_url
REDIS_HOST = settings.redis_host

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# --- Flask App ---
app = Flask(__name__)
start_time_dt = datetime.now()

# --- State for TPS Calculation ---
transaction_times = deque()
last_check_time = datetime.now()
last_total_verified = 0


def get_redis_connection():
    return redis.Redis(host=REDIS_HOST, port=settings.redis_port, decode_responses=True)


def calculate_tps_from_last_minute(total_verified: int):
    global last_check_time, last_total_verified, transaction_times

    # --- Calculate TPS over the last minute ---
    current_time = datetime.now()
    new_transactions = total_verified - last_total_verified

    if new_transactions > 0:
        # Record a timestamp for each new transaction to get a more accurate TPS
        for _ in range(new_transactions):
            transaction_times.append(current_time)

    # Remove timestamps older than 60 seconds
    while (
        transaction_times and (current_time - transaction_times[0]).total_seconds() > 60
    ):
        transaction_times.popleft()

    # Calculate TPS based on the number of transactions in the last minute
    time_window = (current_time - start_time_dt).total_seconds()

    if time_window < 60:
        # If the app has been running for less than a minute, use the total elapsed time
        avg_tps = len(transaction_times) / time_window if time_window > 0 else 0
    else:
        # Otherwise, calculate TPS over the last 60 seconds
        avg_tps = len(transaction_times) / 60.0
    AVERAGE_TPS.set(avg_tps)

    # Update state for next calculation
    last_total_verified = total_verified
    return avg_tps


@app.route("/")
def home():
    return render_template("home.html")


@app.route("/data")
def data():
    historical_total = 0
    historical_invalid = 0
    try:
        db = SessionLocal()
        inspector = inspect(db.get_bind())
        if inspector.has_table("transactions"):
            historical_total = (
                db.query(func.count(Transaction.transaction_id)).scalar() or 0
            )
            historical_invalid = (
                db.query(func.count(Transaction.transaction_id))
                .filter(Transaction.current_state == "invalid")
                .scalar()
                or 0
            )
    except Exception:
        pass
    finally:
        if "db" in locals():
            db.close()

    redis_client = get_redis_connection()
    realtime_validated = int(redis_client.get("total_validated_realtime") or 0)
    realtime_invalid = int(redis_client.get("total_invalid_realtime") or 0)

    total_verified = historical_total + realtime_validated + realtime_invalid
    invalid_count = historical_invalid + realtime_invalid
    correct_count = total_verified - invalid_count

    avg_tps = calculate_tps_from_last_minute(total_verified=total_verified)

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
    redis_client = get_redis_connection()
    transactions_json = redis_client.lrange("recent_invalid_transactions", 0, 19)
    transactions = [json.loads(t) for t in transactions_json]
    return render_template("invalid.html", transactions=transactions)


if __name__ == "__main__":
    start_metrics_server(8005)
    app.run(host="0.0.0.0", port=5000, debug=True)
