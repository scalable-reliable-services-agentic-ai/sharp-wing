import json
import logging
from fastmcp import FastMCP
import os
import sys

# Dynamically find the project root and add it to Python's path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.insert(0, project_root)

from src.config import settings
import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-server")

# Initialize the FastMCP Server
mcp = FastMCP("FraudDetectionAgent")


# Connection Helpers
async def get_db_connection():
    """Helper to get an async connection to TimescaleDB"""
    engine = create_async_engine(settings.async_database_url)
    return engine


async def get_redis_client():
    """Helper to get an async connection to Redis"""
    return aioredis.from_url(f"redis://{settings.redis_host}", decode_responses=True)


# MCP Tools for the LLM Agent

@mcp.tool()
async def get_user_history(client_id: int, limit: int = 5) -> str:
    """
    Fetches the recent transaction history for a specific user
    we use this tool to establish a baseline of the user's normal behavior
    """
    logger.info(f"Agent requested history for client {client_id}")
    engine = await get_db_connection()

    try:
        async with engine.connect() as conn:
            # Query the TimescaleDB hypertable
            query = text("""
                         SELECT transaction_id, amount, location, timestamp_iso, is_fraud
                         FROM transactions
                         WHERE client_id = :client_id
                         ORDER BY timestamp_ms DESC LIMIT :limit
                         """)
            result = await conn.execute(query, {"client_id": client_id, "limit": limit})
            history = [dict(row._mapping) for row in result]

            return json.dumps(history, default=str)
    except Exception as e:
        return f"Error fetching history: {str(e)}"
    finally:
        await engine.dispose()


@mcp.tool()
async def evaluate_impossible_travel(client_id: int, current_location: str, current_timestamp_ms: int) -> str:
    """
    Evaluates if the user's current transaction location conflicts geographically
    with their last known location in Redis, indicating a potential Account Takeover
    """
    logger.info(f"Agent evaluating impossible travel for client {client_id}")
    redis_client = await get_redis_client()

    try:
        # Fetch the last known location from the Redis profile updated by the Validator
        profile = await redis_client.hgetall(f"client:{client_id}:profile")

        if not profile or "last_location" not in profile:
            return "Insufficient data: No previous location history found for this user."

        last_location = profile["last_location"]
        last_seen_ts = int(profile.get("last_seen_ts", 0))

        time_diff_minutes = (current_timestamp_ms - last_seen_ts) / (1000 * 60)

        # Here we would normally calculate actual Haversine distance between coords
        # For now, we return the raw data so the LLM can reason about it
        return json.dumps({
            "previous_location": last_location,
            "current_location": current_location,
            "time_elapsed_minutes": round(time_diff_minutes, 2),
            "assessment": "Agent must determine if travel between these coordinates is possible in the given time."
        })
    finally:
        await redis_client.close()


@mcp.tool()
async def evaluate_daily_velocity(client_id: int, current_timestamp_ms: int) -> str:
    """
    Calculates the total transaction volume (sum of amounts) for a user over the last 24 hours
    we use this tool to detect 'Smurfing' patterns where a user tries to stay just under reporting limits (like $10000)
    """
    logger.info(f"Agent checking 24h velocity for client {client_id}")
    engine = await get_db_connection()

    # 24 hours in milliseconds (24 * 60 * 60 * 1000)
    twenty_four_hours_ago = current_timestamp_ms - 86400000

    try:
        async with engine.connect() as conn:
            query = text("""
                         SELECT COUNT(*) as transaction_count, COALESCE(SUM(amount), 0) as total_volume
                         FROM transactions
                         WHERE client_id = :client_id
                           AND timestamp_ms >= :twenty_four_hours_ago
                           AND timestamp_ms <= :current_timestamp_ms
                         """)

            result = await conn.execute(query, {
                "client_id": client_id,
                "twenty_four_hours_ago": twenty_four_hours_ago,
                "current_timestamp_ms": current_timestamp_ms
            })

            # Fetch the first (and only) row
            row = result.fetchone()

            velocity_data = {
                "client_id": client_id,
                "time_window_hours": 24,
                "transaction_count": row.transaction_count,
                "total_volume": float(row.total_volume),
                # We give the LLM a gentle hint if it's suspiciously close to 10k!
                "smurfing_risk_flag": 9000 <= float(row.total_volume) < 10000
            }

            return json.dumps(velocity_data)
    except Exception as e:
        return f"Error calculating velocity: {str(e)}"
    finally:
        await engine.dispose()


@mcp.tool()
async def escalate_to_operator(transaction_id: int, agent_reasoning: str, confidence_score: float) -> str:
    """
    If the agent's confidence in accepting/rejecting the transaction is below threshold (for example, < 0.85),
    use this tool to route the case to a human operator queue (with dashboard update)
    """
    logger.warning(f"Agent escalating transaction {transaction_id} (Confidence: {confidence_score})")

    # TODO: In the future, this tool could publish a message to the 'human-review-required' Kafka topic

    return f"Success: Transaction {transaction_id} escalated to human operator. Reason logged: {agent_reasoning}"


if __name__ == "__main__":
    # Start the FastMCP server
    logger.info("Starting Fraud Detection MCP Server...")
    mcp.run()