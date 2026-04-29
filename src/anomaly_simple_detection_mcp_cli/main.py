import json
import time
import asyncio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as aioredis
from src.config import settings, configure_logging

logger = configure_logging(__name__)

# --- Environment Variables ---
KAFKA_BROKER = settings.kafka_broker
IN_TOPIC = settings.kafka_validated_transactions_topic
FINAL_TOPIC = settings.kafka_final_transactions_topic
REDIS_HOST = settings.redis_host

