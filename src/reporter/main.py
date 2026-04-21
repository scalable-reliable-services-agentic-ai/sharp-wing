import os
import json
from datetime import datetime
from flask import Flask, jsonify, render_template
from sqlalchemy import create_engine, func, Column, BigInteger, String, Float, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.dialects.postgresql import JSONB
import redis

# --- Environment & DB Setup ---
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
REDIS_HOST = os.environ["REDIS_HOST"]

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

from src.database.models import Base, Transaction


# --- Flask App ---
app = Flask(__name__)
start_time_dt = datetime.now()


def get_redis_connection():
    redis_port = int(os.environ["REDIS_PORT"])
    return redis.Redis(host=REDIS_HOST, port=redis_port, decode_responses=True)


@app.route("/")
def home():
    return render_template("home.html")


@app.route("/data")
def data():
    historical_total = 0
    historical_invalid = 0
    try:
        db = SessionLocal()
        # Check if the table exists first
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
        # This will catch connection errors if the DB isn't fully ready
        pass  # Defaults will be used
    finally:
        if "db" in locals():
            db.close()

    redis_client = get_redis_connection()
    realtime_validated = int(redis_client.get("total_validated_realtime") or 0)
    realtime_invalid = int(redis_client.get("total_invalid_realtime") or 0)

    total_verified = historical_total + realtime_validated + realtime_invalid
    invalid_count = historical_invalid + realtime_invalid
    correct_count = total_verified - invalid_count

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
    redis_client = get_redis_connection()
    transactions_json = redis_client.lrange("recent_invalid_transactions", 0, 19)
    transactions = [json.loads(t) for t in transactions_json]
    return render_template("invalid.html", transactions=transactions)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
