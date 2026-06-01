import asyncio
import sys
import os
import subprocess
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text
from src.config import settings


async def wait_for_data_diversity():
    engine = create_async_engine(settings.async_database_url)
    print("Waiting for transaction history to capture varied behavior (max amount > $500)...", flush=True)
    while True:
        try:
            async with engine.connect() as conn:
                res = await conn.execute(text("SELECT COALESCE(MAX(amount), 0) FROM transactions;"))
                max_amount = res.scalar() or 0

                res_count = await conn.execute(text("SELECT COUNT(*) FROM transactions;"))
                count = res_count.scalar() or 0

                if count > 500 and max_amount > 500:
                    print(f"Diverse dataset verified ({count} records, max amount: ${max_amount}).", flush=True)
                    break
        except Exception as e:
            print(f"Waiting for database connection... {str(e)}", flush=True)
        await asyncio.sleep(3)
    await engine.dispose()


def main():
    # 1. Wait for database records to accumulate
    asyncio.run(wait_for_data_diversity())

    # 2. Train the baseline model if the artifact is missing
    model_path = "/app/src/ml_service/models/fraud_model.pkl"
    if not os.path.exists(model_path):
        print("Shared model artifact missing. Training baseline model...", flush=True)
        subprocess.run(["python", "-m", "src.ml_service.train"], check=True)

    # 3. Handoff control to the main retrain worker service execution thread
    print("Baseline model secured. Launching active Retrain Worker loop...", flush=True)
    os.execvp("python", ["python", "-m", "src.ml_service.retrain_worker"])


if __name__ == "__main__":
    main()
