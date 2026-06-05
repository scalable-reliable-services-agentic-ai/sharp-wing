from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml
from typing import Any, Dict


def yaml_config_source(*args, **kwargs) -> Dict[str, Any]:
    """
    A settings source that loads variables from the specific llm.yaml file.
    """
    try:
        with open("config/llm.yaml", "r") as f:
            data = yaml.safe_load(f) or {}
        config_data = {
            "litellm_proxy_url": data.get("LITELLM_PROXY_URL"),
            "litellm_gemini_model": data.get("MODEL", {}).get("gemini", {}).get("name"),
            "litellm_mistral_model": data.get("MODEL", {})
            .get("mistral", {})
            .get("name"),
        }
        return {k: v for k, v in config_data.items() if v is not None}
    except (FileNotFoundError, yaml.YAMLError):
        return {}


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=("common.env", ".env"), env_file_encoding="utf-8"
    )

    # Kafka Configuration
    kafka_broker: str
    kafka_raw_transactions_topic: str
    kafka_validated_transactions_topic: str
    kafka_invalid_transactions_topic: str
    kafka_noanomaly_transactions_topic: str
    kafka_anomaly_detected_transactions_topic: str
    kafka_human_review_required_topic: str
    kafka_final_transactions_topic: str
    kafka_human_resolved_topic: str

    # Redis Configuration
    redis_host: str
    redis_port: int

    # TimescaleDB/PostgreSQL Configuration
    postgres_user: str
    postgres_password: str
    postgres_db: str
    postgres_port: int
    db_host: str

    # Batch Ingestor Configuration
    batch_size: int
    batch_interval: int

    # Generator Configuration
    generator_min_sleep_ms: float
    generator_max_sleep_ms: float
    generator_cycle_length_seconds: int

    # Validator Configuration
    validator_min_amount: int
    validator_max_amount: int

    # LiteLLM Configuration
    litellm_proxy_url: str
    litellm_api_key: str
    litellm_gemini_model: str
    litellm_mistral_model: str

    # Dashboard Security
    jwt_secret_key: str
    dashboard_admin_user: str
    dashboard_admin_password: str

    @property
    def database_url(self) -> str:
        return f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}@{self.db_host}/{self.postgres_db}"

    @property
    def async_database_url(self) -> str:
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.db_host}/{self.postgres_db}"

    # @classmethod
    # def settings_customise_sources(
    #     cls,
    #     settings_cls,
    #     init_settings,
    #     env_settings,
    #     dotenv_settings,
    #     file_secret_settings,
    # ):
    #     return (
    #         init_settings,
    #         env_settings,
    #         dotenv_settings,
    #         yaml_config_source,
    #         file_secret_settings,
    #     )


settings = Settings()


def configure_logging(name: str):
    import logging

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(name)
