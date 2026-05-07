from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncpg
from src.config import settings, configure_logging

logger = configure_logging(__name__)

app = FastAPI(title="Fraud Triage HITL API")

# Allow React frontend to communicate with this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ResolutionRequest(BaseModel):
    decision: str


async def get_db_connection():
    return await asyncpg.connect(
        host="localhost", # Keep localhost so your Mac can talk to Docker
        port=getattr(settings, "postgres_port", 5432),
        user=getattr(settings, "postgres_user", "postgres"),
        password=getattr(settings, "postgres_password", "password"),
        database=getattr(settings, "postgres_db", "fraud_detection_db"),
    )


@app.get("/api/queue")
async def get_pending_queue():
    """Fetches transactions flagged for human review."""
    conn = await get_db_connection()
    try:
        query = """
                SELECT transaction_id                                                           as id, \
                       amount, \
                       location, \
                       COALESCE(CAST(jsonb_extract_path_text(history::jsonb, 'system_2', 'confidence') AS FLOAT), \
                                0.0)                                                            as confidence, \
                       COALESCE(jsonb_extract_path_text(history::jsonb, 'system_2', 'reasoning'), \
                                'No AI reasoning found')                                        as ai_reasoning, \
                       COALESCE(CAST(jsonb_extract_path_text(history::jsonb, 'observer_evaluation', \
                                                             'reasoning_grade') AS INTEGER), 0) as observer_grade, \
                       COALESCE(jsonb_extract_path_text(history::jsonb, 'observer_evaluation', 'critique'), \
                                'No Observer critique found')                                   as observer_critique
                FROM transactions
                WHERE current_state = 'ESCALATED'
                ORDER BY timestamp_ms DESC LIMIT 50;
                """
        rows = await conn.fetch(query)
        queue = [dict(row) for row in rows]

        # KPI Queries
        total_processed = await conn.fetchval("SELECT COUNT(*) FROM transactions;")
        auto_denied = await conn.fetchval("SELECT COUNT(*) FROM transactions WHERE current_state = 'AI_RESOLVED';")

        auto_rate = "0.0%"
        if total_processed and total_processed > 0:
            rate = ((total_processed - len(queue)) / total_processed) * 100
            auto_rate = f"{rate:.1f}%"

        return {
            "queue": queue,
            "kpis": {
                "total_processed": total_processed or 0,
                "auto_denied": auto_denied or 0,
                "automation_rate": auto_rate
            }
        }
    except Exception as e:
        logger.error(f"DB Error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")
    finally:
        await conn.close()  # Cleanly close the single connection


@app.post("/api/resolve/{tx_id}")
async def resolve_transaction(tx_id: str, request: ResolutionRequest):
    """Updates the database with the human's final decision."""
    conn = await get_db_connection()
    try:
        new_state = 'RESOLVED_SAFE' if request.decision == 'Safe' else 'RESOLVED_FRAUD'

        query = """
                UPDATE transactions
                SET current_state = $1
                WHERE transaction_id = $2 \
                """
        await conn.execute(query, new_state, tx_id)

        logger.info(f"Human resolved TX {tx_id} as {request.decision}")
        return {"status": "success", "message": f"Transaction {tx_id} updated."}
    except Exception as e:
        logger.error(f"Failed to update TX {tx_id}: {e}")
        raise HTTPException(status_code=500, detail="Update failed")
    finally:
        await conn.close()
