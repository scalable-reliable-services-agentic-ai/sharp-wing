import json
import logging
import os
import sys
from mcp.server.fastmcp import FastMCP
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
import redis.asyncio as aioredis
from src.config import settings

# Dynamically find the project root and add it to Python's path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "../../"))
sys.path.insert(0, project_root)


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-server")

# Initialize the FastMCP Server
mcp = FastMCP("Telemetry-Observability-Agent")


# Connection Helpers
async def get_db_connection():
    """Helper to get an async connection to TimescaleDB"""
    return create_async_engine(settings.async_database_url)


# Tool 1: Baseline History (Catches StolenCard & ATO)
@mcp.tool()
async def get_user_history(client_id: int, limit: int = 5) -> str:
    """
    Fetches the recent transaction history for a specific user
    Use this to establish a baseline of the user's normal behavior
    (normal amounts, normal locations) to detect Stolen Cards or ATOs
    """
    logger.info(f"Agent requested history for client {client_id}")
    engine = await get_db_connection()

    try:
        async with engine.connect() as conn:
            query = text("""
                         SELECT transaction_id, amount, location, timestamp_iso, is_fraud
                         FROM transactions
                         WHERE client_id = :client_id
                         ORDER BY timestamp_ms DESC LIMIT :limit
                         """)
            result = await conn.execute(query, {"client_id": client_id, "limit": limit})
            history = [dict(row._mapping) for row in result]

            if not history:
                return json.dumps({"status": "no_history_found"})

            return json.dumps(history, default=str)
    except Exception as e:
        return json.dumps({"error": f"Error fetching history: {str(e)}"})
    finally:
        await engine.dispose()


# Tool 2: Impossible Travel Evaluator
@mcp.tool()
async def evaluate_impossible_travel(
    client_id: int, current_location: str, current_timestamp_ms: int
) -> str:
    """
    Evaluates if the user's current transaction location conflicts geographically
    with their last known location in the database
    """
    logger.info(f"Agent evaluating impossible travel for client {client_id}")
    engine = await get_db_connection()

    try:
        async with engine.connect() as conn:
            # Get the very last transaction before this one
            query = text("""
                         SELECT location, timestamp_ms
                         FROM transactions
                         WHERE client_id = :client_id
                           AND timestamp_ms < :current_timestamp_ms
                         ORDER BY timestamp_ms DESC LIMIT 1
                         """)
            result = await conn.execute(
                query,
                {"client_id": client_id, "current_timestamp_ms": current_timestamp_ms},
            )
            row = result.fetchone()

            if not row:
                return json.dumps(
                    {"status": "Insufficient data: No previous location history found."}
                )

            last_location = row.location
            last_seen_ts = row.timestamp_ms
            time_diff_minutes = (current_timestamp_ms - last_seen_ts) / (1000 * 60)

            return json.dumps(
                {
                    "previous_location": last_location,
                    "current_location": current_location,
                    "time_elapsed_minutes": round(time_diff_minutes, 2),
                    "assessment": "Agent must determine if travel between these coordinates is possible in the given time.",
                }
            )
    except Exception as e:
        return json.dumps({"error": f"Error calculating travel: {str(e)}"})
    finally:
        await engine.dispose()


# Tool 3: Velocity & Smurfing Check
@mcp.tool()
async def evaluate_daily_velocity(client_id: int, current_timestamp_ms: int) -> str:
    """
    Calculates the total transaction volume (sum of amounts) for a user over the last 24 hours
    Use this tool to detect 'Smurfing' patterns (staying just under $10000 reporting limits)
    """
    logger.info(f"Agent checking 24h velocity for client {client_id}")
    engine = await get_db_connection()
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
            result = await conn.execute(
                query,
                {
                    "client_id": client_id,
                    "twenty_four_hours_ago": twenty_four_hours_ago,
                    "current_timestamp_ms": current_timestamp_ms,
                },
            )
            row = result.fetchone()

            return json.dumps(
                {
                    "client_id": client_id,
                    "time_window_hours": 24,
                    "transaction_count": row.transaction_count,
                    "total_volume": float(row.total_volume),
                }
            )
    except Exception as e:
        return json.dumps({"error": f"Error calculating velocity: {str(e)}"})
    finally:
        await engine.dispose()


# Tool 4: System Health Check (Observability)
@mcp.tool()
async def check_system_health() -> str:
    """
    Check the health of the Database and Redis cache
    Use this if data is missing to determine if there is an IT infrastructure outage
    """
    logger.info("Agent requested System Health Check")
    health = {"postgres": "offline", "redis": "offline"}

    # Check DB
    try:
        engine = await get_db_connection()
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        health["postgres"] = "online"
        await engine.dispose()
    except Exception:
        pass

    # Check Redis
    try:
        redis_client = aioredis.from_url(
            f"redis://{settings.redis_host}:{settings.redis_port}"
        )
        await redis_client.ping()
        await redis_client.close()
        health["redis"] = "online"
    except Exception:
        pass

    return json.dumps(health)


if __name__ == "__main__":
    logger.info("Starting Telemetry & Observability MCP Server...")
    mcp.run()
