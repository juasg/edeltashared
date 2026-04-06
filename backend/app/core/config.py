from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Postgres
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "edeltashared"
    postgres_user: str = "edeltashared"
    postgres_password: str = "changeme_postgres"

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"

    # HANA
    hana_host: str = ""
    hana_port: int = 30015
    hana_user: str = ""
    hana_password: str = ""
    hana_max_connections: int = 3
    hana_query_timeout_ms: int = 10000

    # Security
    secret_key: str = "changeme_secret_key_min_32_chars_long"
    encryption_key: str = "changeme_encryption_key_32_bytes"

    # CORS
    cors_allowed_origins: str = "http://localhost:5173"  # Comma-separated

    # Backend
    backend_host: str = "0.0.0.0"
    backend_port: int = 8000

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def database_url_sync(self) -> str:
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
