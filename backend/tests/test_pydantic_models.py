"""Tests for Pydantic request/response models."""

import uuid

import pytest
from pydantic import ValidationError

from app.models.config import (
    ConnectorConfigCreate,
    FieldMapping,
    ReplicationConfigCreate,
)
from app.models.job import InitialLoadRequest


def test_connector_config_valid():
    config = ConnectorConfigCreate(
        type="snowflake",
        name="My Snowflake",
        credentials={"account": "abc123", "user": "admin"},
    )
    assert config.type == "snowflake"
    assert config.connection_pool_size == 5


def test_connector_config_invalid_type():
    with pytest.raises(ValidationError):
        ConnectorConfigCreate(
            type="mysql",  # Not allowed
            name="Bad Type",
            credentials={},
        )


def test_replication_config_valid():
    config = ReplicationConfigCreate(
        source_table_schema="SAPSR3",
        source_table_name="SALES_ORDERS",
        connector_config_id=uuid.uuid4(),
        target_table_name="sales_orders",
        field_mappings=[
            FieldMapping(source="ORDER_ID", target="order_id"),
            FieldMapping(source="TOTAL_AMOUNT", target="total", transform="cast:float"),
        ],
        primary_key_fields=["ORDER_ID"],
    )
    assert config.replication_mode == "cdc"
    assert len(config.field_mappings) == 2


def test_replication_config_empty_pk_fails():
    with pytest.raises(ValidationError):
        ReplicationConfigCreate(
            source_table_schema="SAPSR3",
            source_table_name="SALES_ORDERS",
            connector_config_id=uuid.uuid4(),
            target_table_name="sales_orders",
            field_mappings=[FieldMapping(source="ORDER_ID", target="order_id")],
            primary_key_fields=[],  # Must have at least 1
        )


def test_initial_load_request_defaults():
    req = InitialLoadRequest(config_id=uuid.uuid4())
    assert req.chunk_size == 10000
    assert req.max_hana_cpu == 60


def test_initial_load_request_bounds():
    with pytest.raises(ValidationError):
        InitialLoadRequest(config_id=uuid.uuid4(), chunk_size=500)  # Below 1000 min

    with pytest.raises(ValidationError):
        InitialLoadRequest(config_id=uuid.uuid4(), max_hana_cpu=95)  # Above 90 max
