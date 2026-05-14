import json
import os
import asyncio
from pathlib import Path
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from src.config import settings, configure_logging
from openai import AsyncOpenAI
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from prometheus_client import Counter
from src.prometheus_metrics.metrics import start_metrics_server

logger = configure_logging(__name__)


# --- Metrics Definition ---
HUMAN_REVIEW_CNT = Counter(
    "human_review_required_transactions",
    "Number of transactions requiring human review",
)

AUTO_PROCESSED_CNT = Counter(
    "auto_processed_transactions",
    "Number of transactions automatically processed by the agent",
)


MODEL_PROXY = settings.litellm_proxy_url
MODEL_KEY = settings.litellm_api_key
MODEL_NAME = settings.litellm_gemini_model

PROMPT_DIR = Path(__file__).parent / "prompts"
SYSTEM_PROMPT = (PROMPT_DIR / "system.md").read_text()
logger.info("System prompt loaded successfully.")

# Funnel Topics
KAFKA_BROKER = settings.kafka_broker
IN_TOPIC = settings.kafka_anomaly_detected_transactions_topic
OUT_REVIEW_TOPIC = settings.kafka_human_review_required_topic
OUT_FINAL_TOPIC = settings.kafka_final_transactions_topic


async def triage_transaction(message, producer, session, openai_tools, llm_client):
    transaction_data = message.value
    tx_id = transaction_data.get("transaction_id")
    logger.info(f"Agent investigating transaction: {tx_id}")
    logger.info(
        f"System 1 Reasons: {transaction_data.get('system_1_reasons', 'Unknown')}"
    )

    try:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(transaction_data)},
        ]

        # The Tool Calling Loop with Economic Guardrails
        max_steps = 5
        current_step = 0

        while current_step < max_steps:
            response = await llm_client.chat.completions.create(
                model=MODEL_NAME,
                messages=messages,
                tools=openai_tools,
                temperature=0.0,
            )

            response_message = response.choices[0].message
            messages.append(response_message)

            # If no tools are requested, the LLM has made its decision
            if not response_message.tool_calls:
                break

            for tool_call in response_message.tool_calls:
                tool_name = tool_call.function.name
                tool_args = json.loads(tool_call.function.arguments)

                logger.info(f"Agent requested tool: {tool_name} with args {tool_args}")
                result = await session.call_tool(tool_name, arguments=tool_args)

                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "name": tool_name,
                        "content": result.content[0].text,
                    }
                )

            current_step += 1

        # If the loop maxed out, force a human escalation
        if current_step >= max_steps:
            logger.warning(
                f"Agent exceeded max reasoning steps ({max_steps}). Forcing Human Escalation."
            )
            # Force the LLM output to demand human review
            llm_output = {
                "requires_human_review": True,
                "reasoning": "Exceeded maximum reasoning steps. Possible loop detected.",
            }
        else:
            # Final Parse if it finished naturally
            llm_output = json.loads(response_message.content)

        logger.info(f"Final Agent Decision: {llm_output}")
        transaction_data["agentic_evaluation"] = llm_output

        # Route based on the LLM's confidence
        if llm_output.get("requires_human_review", True):
            await producer.send_and_wait(OUT_REVIEW_TOPIC, transaction_data)
            HUMAN_REVIEW_CNT.inc()
        else:
            await producer.send_and_wait(OUT_FINAL_TOPIC, transaction_data)
            AUTO_PROCESSED_CNT.inc()

    except Exception as e:
        logger.error(f"Error during LLM triage: {e}")


async def main():
    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        group_id="fraud-triage-group",  # New distinct group
        max_poll_records=1,
        max_poll_interval_ms=300000,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    await consumer.start()
    await producer.start()
    logger.info(f"System 2 LLM Triage Agent listening on: {IN_TOPIC}")

    server_params = StdioServerParameters(
        # command="python", args=["-m", "src.mcp_server.server"], env={**os.environ}
        command="python",
        args=["-m", "src.mcp_server.telemetry_observability"],
        env={**os.environ}
    )
    llm_client = AsyncOpenAI(api_key=MODEL_KEY, base_url=MODEL_PROXY)

    try:
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                mcp_tools = await session.list_tools()

                openai_tools = [
                    {
                        "type": "function",
                        "function": {
                            "name": t.name,
                            "description": t.description,
                            "parameters": t.inputSchema,
                        },
                    }
                    for t in mcp_tools.tools
                ]
                logger.info("MCP Server initialized and tools loaded successfully.")

                async for message in consumer:
                    await triage_transaction(
                        message, producer, session, openai_tools, llm_client
                    )

    except Exception as e:
        logger.error(f"Fatal error in main loop: {e}")
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    start_metrics_server(8003)
    asyncio.run(main())
