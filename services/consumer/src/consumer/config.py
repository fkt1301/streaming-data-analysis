from pydantic_settings import BaseSettings, SettingsConfigDict


class ConsumerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CONSUMER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    kafka_bootstrap_servers: str = "localhost:9092"
    schema_registry_url: str = "http://localhost:8081"
    kafka_topic: str = "users_created"
    dlq_topic: str = "users_created.dlq"

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "streaming"
    postgres_user: str = "streaming"
    postgres_password: str = "streaming"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
