from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=("common.env", ".env"), env_file_encoding="utf-8"
    )

    # Kafka Configuration
    kafka_broker: str
    kafka_raw_transactions_topic: str
    kafka_validated_transactions_topic: str
    kafka_human_review_required_topic: str
    kafka_final_transactions_topic: str

    # Redis Configuration
    redis_host: str
    redis_port: int

    # TimescaleDB/PostgreSQL Configuration
    postgres_user: str
    postgres_password: str
    postgres_db: str
    db_host: str

    # Batch Ingestor Configuration
    batch_size: int
    batch_interval: int

    # Generator Configuration
    generator_min_sleep_ms: int
    generator_max_sleep_ms: int

    # Validator Configuration
    validator_min_amount: int
    validator_max_amount: int

    @property
    def database_url(self) -> str:
        return f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}@{self.db_host}/{self.postgres_db}"

    @property
    def async_database_url(self) -> str:
        return f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}@{self.db_host}/{self.postgres_db}"


settings = Settings()
