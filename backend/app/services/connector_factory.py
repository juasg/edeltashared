"""Factory for creating target database connectors."""

import logging

from app.connectors.base import DataConnector
from app.db.models import ConnectorConfig

logger = logging.getLogger("edeltashared.connector_factory")


def create_connector(config: ConnectorConfig) -> DataConnector:
    """Create a DataConnector instance based on the connector type."""
    if config.type == "snowflake":
        from app.connectors.snowflake.writer import SnowflakeConnector
        return SnowflakeConnector(config.credentials_encrypted)

    elif config.type == "aurora":
        from app.connectors.aurora.writer import AuroraConnector
        return AuroraConnector(config.credentials_encrypted)

    elif config.type == "mongodb":
        from app.connectors.mongodb.writer import MongoDBConnector
        return MongoDBConnector(config.credentials_encrypted)

    else:
        raise ValueError(f"Unknown connector type: {config.type}")
