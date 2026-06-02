import json
import os
import asyncio
import re
from pathlib import Path
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from src.config import settings, configure_logging
from openai import AsyncOpenAI
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from prometheus_client import Counter
from src.prometheus_metrics.metrics import start_metrics_server

logger = configure_logging(__name__)

# Prometheus Metrics Definition
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
OBSERVER_PROMPT = (PROMPT_DIR / "observer_system.md").read_text()
logger.info("System prompts loaded successfully.")

# Funnel Topics
KAFKA_BROKER = settings.kafka_broker
IN_TOPIC = settings.kafka_anomaly_detected_transactions_topic
OUT_REVIEW_TOPIC = settings.kafka_human_review_required_topic
OUT_FINAL_TOPIC = settings.kafka_final_transactions_topic

# Confidence Thresholds
AUTO_APPROVE_THRESHOLD = 0.30
AUTO_DENY_THRESHOLD = 0.85


def parse_llm_json(raw_text: str) -> dict:
    """Bulletproof JSON extractor that ignores conversational text and markdown"""
    if not raw_text:
        return {}

    clean_text = raw_text.strip()

    # 1. Try to extract content inside markdown backticks if they exist
    match = re.search(r"\`\`\`(?:json)?\s*(.*?)\s*\`\`\`", clean_text, re.DOTALL)
    if match:
        clean_text = match.group(1)

    # 2. Find the first '{' and the last '}' to ignore any chatty preamble
    start = clean_text.find("{")
    end = clean_text.rfind("}")

    if start != -1 and end != -1:
        clean_text = clean_text[start: end + 1]

    try:
        return json.loads(clean_text)
    except json.JSONDecodeError as e:
        logger.error(f"FATAL JSON Parse Error. Raw Text: {raw_text}")
        raise e


async def run_observer_evaluation(transaction_data, triage_analysis, tool_history, llm_client):
    """LLM-as-a-Judge: Evaluates the Triage Agent's reasoning"""
    logger.info(f"Observer Agent evaluating triage logic for TX: {transaction_data.get('transaction_id')}")

    payload = {
        "transaction_data": transaction_data,
        "investigation_history": str(tool_history),
        "triage_analysis": triage_analysis,
    }

    messages = [
        {"role": "system", "content": OBSERVER_PROMPT},
        {"role": "user", "content": json.dumps(payload)},
    ]

    try:
        response = await llm_client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            temperature=0.0,
            response_format={"type": "json_object"},
        )

        # Use our bulletproof parser
        observer_output = parse_llm_json(response.choices[0].message.content)

        logger.info(f"Observer Grade: {observer_output.get('reasoning_grade')}/5 - {observer_output.get('critique')}")
        return observer_output
    except Exception as e:
        logger.error(f"Observer Agent failed: {e}")
        return {
            "reasoning_grade": 0,
            "critique": "Observer execution failed.",
            "force_human_review": True,
        }


async def triage_transaction(message, producer, session, openai_tools, llm_client):
    transaction_data = message.value
    tx_id = transaction_data.get("transaction_id")
    logger.info(f"Agent investigating transaction: {tx_id}")
    logger.info(f"System 1 Reasons: {transaction_data.get('system_1_reasons', 'Unknown')}")

    try:
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(transaction_data)},
        ]

        max_steps = 5
        current_step = 0
        response_message = None

        while current_step < max_steps:
            # Rate-Limit Resilient Retry Block
            max_retries = 5
            backoff_in_seconds = 15
            response = None

            for attempt in range(max_retries):
                try:
                    response = await llm_client.chat.completions.create(
                        model=MODEL_NAME,
                        messages=messages,
                        tools=openai_tools,
                        temperature=0.0,
                    )
                    break
                except Exception as e:
                    if "429" in str(e) and attempt < max_retries - 1:
                        logger.warning(
                            f"Rate limited (429) during Agent Triage loop. Retrying in {backoff_in_seconds}s (Attempt {attempt + 1}/{max_retries})...")
                        await asyncio.sleep(backoff_in_seconds)
                        backoff_in_seconds *= 2
                    else:
                        logger.error(f"Execution crashed or exhausted retries on step {current_step}: {e}")
                        raise e

            response_message = response.choices[0].message
            messages.append(response_message)

            if not response_message.tool_calls:
                break

            for tool_call in response_message.tool_calls:
                tool_name = tool_call.function.name

                args_str = tool_call.function.arguments
                tool_args = json.loads(args_str) if args_str and args_str.strip() else {}

                logger.info(f"Agent requested tool: {tool_name}")
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

        if current_step >= max_steps:
            logger.warning("Agent exceeded max steps. Forcing Escalation.")
            llm_output = {
                "confidence": 0.5,
                "is_fraud": False,
                "requires_human_review": True,
                "reasoning": "Loop detected.",
            }
        else:
            # Use our bulletproof parser
            llm_output = parse_llm_json(response_message.content)

        # PHASE 2: Observer Evaluation (LLM-as-a-Judge) with integrated backoff logic
        max_observer_retries = 3
        observer_backoff = 4
        observer_output = None

        for attempt in range(max_observer_retries):
            try:
                observer_output = await run_observer_evaluation(transaction_data, llm_output, messages, llm_client)
                break
            except Exception as e:
                if "429" in str(e) and attempt < max_observer_retries - 1:
                    logger.warning(f"Rate limited (429) during Observer Judge loop. Retrying in {observer_backoff}s...")
                    await asyncio.sleep(observer_backoff)
                    observer_backoff *= 2
                else:
                    logger.error(f"Observer processing failed permanently: {e}")
                    observer_output = {
                        "reasoning_grade": 0,
                        "critique": "Observer execution failed due to rate limits.",
                        "force_human_review": True,
                    }
                    break

        transaction_data["agentic_evaluation"] = llm_output
        transaction_data["observer_evaluation"] = observer_output

        # PHASE 3: Routing Logic & Thresholds
        confidence = float(llm_output.get("confidence", 0.5))
        requires_human = llm_output.get("requires_human_review", False)
        observer_veto = observer_output.get("force_human_review", False)

        if requires_human or observer_veto:
            logger.warning(f"Routing to HITL: Veto={observer_veto}, Requested={requires_human}, Conf={confidence}")
            await producer.send_and_wait(OUT_REVIEW_TOPIC, transaction_data)
            HUMAN_REVIEW_CNT.inc()

        elif confidence >= AUTO_DENY_THRESHOLD:
            logger.info(f"Auto-Denying TX: Conf={confidence} >= {AUTO_DENY_THRESHOLD}")
            await producer.send_and_wait(OUT_FINAL_TOPIC, transaction_data)
            AUTO_PROCESSED_CNT.inc()

        elif confidence <= AUTO_APPROVE_THRESHOLD:
            logger.info(f"Auto-Approving TX: Conf={confidence} <= {AUTO_APPROVE_THRESHOLD}")
            await producer.send_and_wait(OUT_FINAL_TOPIC, transaction_data)
            AUTO_PROCESSED_CNT.inc()

        else:
            logger.warning(f"Routing to HITL: TX in Grey Zone (Conf={confidence})")
            await producer.send_and_wait(OUT_REVIEW_TOPIC, transaction_data)
            HUMAN_REVIEW_CNT.inc()

    except json.JSONDecodeError as e:
        # Structural JSON Parse Failure Fallback Gate
        logger.warning(f"Failed to parse LLM output JSON string structure. Error: {e}", exc_info=True)
        fallback_output = {
            "requires_human_review": True,
            "confidence": 0.5,
            "reasoning": "System forced human review due to unparsable structural LLM text payload output.",
        }
        transaction_data["agentic_evaluation"] = fallback_output
        if "observer_evaluation" not in transaction_data:
            transaction_data["observer_evaluation"] = {"reasoning_grade": 0,
                                                       "critique": "Skipped due to parsing failure.",
                                                       "force_human_review": True}

        await producer.send_and_wait(OUT_REVIEW_TOPIC, transaction_data)
        HUMAN_REVIEW_CNT.inc()

    except Exception as e:
        logger.error(f"Error during LLM triage processing execution context: {e}", exc_info=True)


async def main():
    consumer = AIOKafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",
        group_id="fraud-triage-group",
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
    logger.info(f"System 2 LLM Triage Agent & Observer listening on: {IN_TOPIC}")

    server_params = StdioServerParameters(
        command="python",
        args=["-m", "src.mcp_server.mcp_tools"],
        env={**os.environ},
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
                    await triage_transaction(message, producer, session, openai_tools, llm_client)

    except Exception as e:
        logger.error(f"Fatal error in main loop: {e}", exc_info=True)
    finally:
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    # Expose Metrics Listener Server Endpoint on port 8003 for Prometheus mapping
    start_metrics_server(8003)
    asyncio.run(main())
