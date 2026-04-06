from pydantic_settings import BaseSettings


class AgentSettings(BaseSettings):
    # HANA
    hana_host: str = ""
    hana_port: int = 30015
    hana_user: str = ""
    hana_password: str = ""
    hana_max_connections: int = 3
    hana_query_timeout_ms: int = 10000

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"

    # PostgreSQL (metadata DB)
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "edeltashared"
    postgres_user: str = "edeltashared"
    postgres_password: str = "changeme_postgres"

    # Redis (pub/sub for control plane commands)
    redis_url: str = "redis://localhost:6379/0"

    # Simulator mode (use Postgres instead of HANA)
    simulator_mode: bool = True  # Default True since HANA may not be available

    # Agent behavior
    cdc_poll_interval_seconds: float = 3.0
    cdc_batch_size: int = 1000
    cdc_purge_interval_seconds: int = 3600
    cdc_hana_cpu_threshold: int = 70

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = AgentSettings()
