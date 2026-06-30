from pydantic_settings import BaseSettings, SettingsConfigDict


class ProducerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PRODUCER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    kafka_topic: str = "users_created"
    fetch_interval_seconds: float = 2.0
    randomuser_api_url: str = "https://randomuser.me/api/"
