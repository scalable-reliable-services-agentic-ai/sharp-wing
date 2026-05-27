import json
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
import asyncpg
from aiokafka import AIOKafkaProducer
from jose import jwt, JWTError
from passlib.context import CryptContext

try:
    from src.config import settings, configure_logging
except ImportError:
    from config import settings, configure_logging


logger = configure_logging(__name__)

# SECURITY SETUP
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="api/token")

SECRET_KEY = settings.jwt_secret_key
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60

ADMIN_USERNAME = settings.dashboard_admin_user
ADMIN_PASSWORD_HASH = pwd_context.hash(settings.dashboard_admin_password)


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(token: str = Depends(oauth2_scheme)):
    """Middleware dependency to protect endpoints"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None or username != ADMIN_USERNAME:
            raise credentials_exception
        return username
    except JWTError:
        raise credentials_exception


# KAFKA LIFESPAN
producer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    # broker = "localhost:9092"  # to fix for k8s deploy, there and in other places
    broker = getattr(settings, "kafka_broker", "localhost:9092")
    producer = AIOKafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    try:
        await producer.start()
        logger.info(f"Connected to Kafka at {broker}")
        yield
    except Exception as e:
        logger.error(f"Kafka Connection Failed: {e}")
        yield
    finally:
        if producer:
            await producer.stop()


# APP SETUP
app = FastAPI(title="Fraud Triage HITL API", lifespan=lifespan)

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
        host=getattr(settings, "db_host", "localhost"),  # host="localhost",
        port=getattr(settings, "postgres_port", 5432),
        user=getattr(settings, "postgres_user", "postgres"),
        password=getattr(settings, "postgres_password", "password"),
        database=getattr(settings, "postgres_db", "fraud_detection_db"),
    )


# API ENDPOINTS


@app.post("/api/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """Authenticates the user and returns a JWT"""
    if form_data.username != ADMIN_USERNAME or not verify_password(
        form_data.password, ADMIN_PASSWORD_HASH
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    access_token = create_access_token(data={"sub": form_data.username})
    return {"access_token": access_token, "token_type": "bearer"}


@app.get("/api/queue")
async def get_pending_queue(current_user: str = Depends(get_current_user)):
    """Fetches transactions flagged for human review (Requires JWT)"""
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
                ORDER BY timestamp_ms DESC LIMIT 50; \
                """
        rows = await conn.fetch(query)
        queue = [dict(row) for row in rows]

        total_processed = await conn.fetchval("SELECT COUNT(*) FROM transactions;")
        auto_denied = await conn.fetchval("""
                                          SELECT COUNT(*)
                                          FROM transactions
                                          WHERE current_state = 'AI_RESOLVED'
                                            AND
                                              CAST(jsonb_extract_path_text(history::jsonb, 'system_2', 'is_fraud') AS BOOLEAN) =
                                              true;
                                          """)
        auto_approved = await conn.fetchval("""
                                            SELECT COUNT(*)
                                            FROM transactions
                                            WHERE current_state = 'AI_RESOLVED'
                                              AND
                                                CAST(jsonb_extract_path_text(history::jsonb, 'system_2', 'is_fraud') AS BOOLEAN) =
                                                false;
                                            """)

        auto_rate = "0.0%"
        if total_processed and total_processed > 0:
            rate = ((total_processed - len(queue)) / total_processed) * 100
            auto_rate = f"{rate:.1f}%"

        return {
            "queue": queue,
            "kpis": {
                "total_processed": total_processed or 0,
                "auto_denied": auto_denied or 0,
                "auto_approved": auto_approved or 0,
                "automation_rate": auto_rate,
            },
        }
    except Exception as e:
        logger.error(f"DB Error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")
    finally:
        await conn.close()


@app.post("/api/resolve/{tx_id}")
async def resolve_transaction(
    tx_id: str,
    request: ResolutionRequest,
    current_user: str = Depends(get_current_user),
):
    """Updates DB AND publishes the ground truth to Kafka (Requires JWT)"""
    conn = await get_db_connection()
    try:
        new_state = "RESOLVED_SAFE" if request.decision == "Safe" else "RESOLVED_FRAUD"
        ground_truth_label = "SAFE" if request.decision == "Safe" else "FRAUD"

        update_query = "UPDATE transactions SET current_state = $1 WHERE transaction_id = $2 RETURNING *"
        row = await conn.fetchrow(update_query, new_state, int(tx_id))

        if not row:
            raise HTTPException(status_code=404, detail="Transaction not found")

        ground_truth_payload = {
            "transaction_id": tx_id,
            "human_label": ground_truth_label,
            "original_amount": row["amount"],
            "original_location": row["location"],
            "ai_history": json.loads(row["history"]) if row["history"] else {},
        }

        global producer
        topic_name = getattr(
            settings, "kafka_human_resolved_topic", "fraud_human_resolved"
        )
        await producer.send_and_wait(topic_name, ground_truth_payload)

        logger.info(
            f"Published Ground Truth for {tx_id}: {ground_truth_label} by {current_user}"
        )
        return {"status": "success"}

    except Exception as e:
        logger.error(f"Failed to resolve TX {tx_id}: {e}")
        raise HTTPException(status_code=500, detail="Update failed")
    finally:
        await conn.close()
