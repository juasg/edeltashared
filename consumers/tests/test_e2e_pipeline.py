"""
End-to-end pipeline tests for the consumer transform + quality engines.

Tests TransformEngine, QualityEngine, and EventPipeline integration
without requiring Kafka or external databases.

Run with:
    pytest consumers/tests/test_e2e_pipeline.py -v
"""

import hashlib
import json
import uuid
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import psycopg2
import pytest

import sys
import os

# Add consumers/ to path so imports resolve
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from transform_engine import TransformEngine, mask_email, mask_name, mask_phone, mask_ssn
from quality_engine import QualityEngine, ValidationResult, Violation
from base_consumer import CDCMessage


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PG_DSN = "host=localhost port=5432 dbname=edeltashared user=edeltashared password=changeme_postgres"


def make_cdc_message(
    fields: dict | None = None,
    op: str = "I",
    seq: int = 1,
    table: str = "SALES_ORDERS",
    key: dict | None = None,
) -> CDCMessage:
    return CDCMessage(
        source="hana-sidecar-01",
        schema="SAPSR3",
        table=table,
        op=op,
        key=key or {"ORDER_ID": f"ORD-{uuid.uuid4().hex[:6]}"},
        before=None,
        after=fields,
        ts_ms=1700000000000,
        seq=seq,
    )


# ===========================================================================
# 1. TransformEngine — chained transforms
# ===========================================================================


class TestTransformEngineBasic:
    def test_upper_transform(self):
        engine = TransformEngine([
            {"source_field": "name", "transform_type": "upper", "transform_params": {}, "execution_order": 0},
        ])
        result = engine.apply({"name": "john doe", "age": 30})
        assert result["name"] == "JOHN DOE"
        assert result["age"] == 30  # Untouched

    def test_lower_transform(self):
        engine = TransformEngine([
            {"source_field": "email", "transform_type": "lower", "transform_params": {}, "execution_order": 0},
        ])
        result = engine.apply({"email": "User@Example.COM"})
        assert result["email"] == "user@example.com"

    def test_trim_transform(self):
        engine = TransformEngine([
            {"source_field": "code", "transform_type": "trim", "transform_params": {}, "execution_order": 0},
        ])
        result = engine.apply({"code": "  ABC-123  "})
        assert result["code"] == "ABC-123"

    def test_cast_to_int(self):
        engine = TransformEngine([
            {"source_field": "qty", "transform_type": "cast", "transform_params": {"type": "int"}, "execution_order": 0},
        ])
        result = engine.apply({"qty": "42.7"})
        assert result["qty"] == 42
        assert isinstance(result["qty"], int)

    def test_cast_to_float(self):
        engine = TransformEngine([
            {"source_field": "price", "transform_type": "cast", "transform_params": {"type": "float"}, "execution_order": 0},
        ])
        result = engine.apply({"price": "19.99"})
        assert result["price"] == pytest.approx(19.99)

    def test_cast_to_bool(self):
        engine = TransformEngine([
            {"source_field": "active", "transform_type": "cast", "transform_params": {"type": "bool"}, "execution_order": 0},
        ])
        assert engine.apply({"active": "true"})["active"] is True
        assert engine.apply({"active": "0"})["active"] is False
        assert engine.apply({"active": "yes"})["active"] is True

    def test_hash_sha256(self):
        engine = TransformEngine([
            {"source_field": "ssn", "transform_type": "hash", "transform_params": {"algorithm": "sha256"}, "execution_order": 0},
        ])
        result = engine.apply({"ssn": "123-45-6789"})
        expected = hashlib.sha256(b"123-45-6789").hexdigest()
        assert result["ssn"] == expected

    def test_hash_md5(self):
        engine = TransformEngine([
            {"source_field": "val", "transform_type": "hash", "transform_params": {"algorithm": "md5"}, "execution_order": 0},
        ])
        result = engine.apply({"val": "test"})
        expected = hashlib.md5(b"test").hexdigest()
        assert result["val"] == expected

    def test_substr_transform(self):
        engine = TransformEngine([
            {"source_field": "code", "transform_type": "substr", "transform_params": {"start": 0, "length": 3}, "execution_order": 0},
        ])
        result = engine.apply({"code": "ABCDEF"})
        assert result["code"] == "ABC"

    def test_substr_no_length(self):
        engine = TransformEngine([
            {"source_field": "code", "transform_type": "substr", "transform_params": {"start": 2}, "execution_order": 0},
        ])
        result = engine.apply({"code": "ABCDEF"})
        assert result["code"] == "CDEF"

    def test_replace_transform(self):
        engine = TransformEngine([
            {"source_field": "phone", "transform_type": "replace", "transform_params": {"pattern": "-", "replacement": ""}, "execution_order": 0},
        ])
        result = engine.apply({"phone": "555-123-4567"})
        assert result["phone"] == "5551234567"

    def test_default_transform_fills_null(self):
        engine = TransformEngine([
            {"source_field": "status", "transform_type": "default", "transform_params": {"value": "UNKNOWN"}, "execution_order": 0},
        ])
        result = engine.apply({"status": None, "other": "x"})
        assert result["status"] == "UNKNOWN"

    def test_default_transform_preserves_existing(self):
        engine = TransformEngine([
            {"source_field": "status", "transform_type": "default", "transform_params": {"value": "UNKNOWN"}, "execution_order": 0},
        ])
        result = engine.apply({"status": "ACTIVE"})
        assert result["status"] == "ACTIVE"

    def test_null_input_returns_none(self):
        engine = TransformEngine([
            {"source_field": "x", "transform_type": "upper", "transform_params": {}, "execution_order": 0},
        ])
        assert engine.apply(None) is None

    def test_missing_field_not_transformed(self):
        engine = TransformEngine([
            {"source_field": "nonexistent", "transform_type": "upper", "transform_params": {}, "execution_order": 0},
        ])
        result = engine.apply({"name": "test"})
        assert result == {"name": "test"}

    def test_null_value_passthrough(self):
        """Non-default transforms should pass through None values."""
        engine = TransformEngine([
            {"source_field": "name", "transform_type": "upper", "transform_params": {}, "execution_order": 0},
        ])
        result = engine.apply({"name": None})
        assert result["name"] is None


class TestTransformEngineChaining:
    def test_chained_trim_then_upper(self):
        engine = TransformEngine([
            {"source_field": "name", "transform_type": "trim", "transform_params": {}, "execution_order": 1},
            {"source_field": "name", "transform_type": "upper", "transform_params": {}, "execution_order": 2},
        ])
        result = engine.apply({"name": "  john doe  "})
        assert result["name"] == "JOHN DOE"

    def test_chained_default_then_upper(self):
        engine = TransformEngine([
            {"source_field": "status", "transform_type": "default", "transform_params": {"value": "pending"}, "execution_order": 1},
            {"source_field": "status", "transform_type": "upper", "transform_params": {}, "execution_order": 2},
        ])
        result = engine.apply({"status": None})
        assert result["status"] == "PENDING"

    def test_chained_replace_then_hash(self):
        engine = TransformEngine([
            {"source_field": "ssn", "transform_type": "replace", "transform_params": {"pattern": "-", "replacement": ""}, "execution_order": 1},
            {"source_field": "ssn", "transform_type": "hash", "transform_params": {"algorithm": "sha256"}, "execution_order": 2},
        ])
        result = engine.apply({"ssn": "123-45-6789"})
        expected = hashlib.sha256(b"123456789").hexdigest()
        assert result["ssn"] == expected

    def test_multiple_fields_independent_chains(self):
        engine = TransformEngine([
            {"source_field": "name", "transform_type": "upper", "transform_params": {}, "execution_order": 1},
            {"source_field": "email", "transform_type": "lower", "transform_params": {}, "execution_order": 1},
            {"source_field": "phone", "transform_type": "mask", "transform_params": {"mask_type": "phone"}, "execution_order": 1},
        ])
        result = engine.apply({
            "name": "john",
            "email": "USER@EXAMPLE.COM",
            "phone": "555-123-4567",
        })
        assert result["name"] == "JOHN"
        assert result["email"] == "user@example.com"
        assert "4567" in result["phone"]
        assert "555" not in result["phone"]

    def test_execution_order_respected(self):
        """Transforms with lower execution_order run first."""
        engine = TransformEngine([
            {"source_field": "val", "transform_type": "upper", "transform_params": {}, "execution_order": 10},
            {"source_field": "val", "transform_type": "substr", "transform_params": {"start": 0, "length": 3}, "execution_order": 5},
        ])
        # substr(5) runs first: "hello" -> "hel", then upper(10): "hel" -> "HEL"
        result = engine.apply({"val": "hello world"})
        assert result["val"] == "HEL"


# ===========================================================================
# 2. PII Masking
# ===========================================================================


class TestPIIMasking:
    def test_mask_email(self):
        engine = TransformEngine([
            {"source_field": "email", "transform_type": "mask", "transform_params": {"mask_type": "email"}, "execution_order": 0},
        ])
        result = engine.apply({"email": "john.doe@example.com"})
        masked = result["email"]
        # Should start with 'j' and contain '@'
        assert masked.startswith("j")
        assert "@" in masked
        # Original should not be fully visible
        assert "john.doe" not in masked
        assert masked.endswith(".com")

    def test_mask_phone(self):
        engine = TransformEngine([
            {"source_field": "phone", "transform_type": "mask", "transform_params": {"mask_type": "phone"}, "execution_order": 0},
        ])
        result = engine.apply({"phone": "(555) 123-4567"})
        assert result["phone"] == "***-***-4567"

    def test_mask_ssn(self):
        engine = TransformEngine([
            {"source_field": "ssn", "transform_type": "mask", "transform_params": {"mask_type": "ssn"}, "execution_order": 0},
        ])
        result = engine.apply({"ssn": "123-45-6789"})
        assert result["ssn"] == "***-**-6789"

    def test_mask_name(self):
        engine = TransformEngine([
            {"source_field": "name", "transform_type": "mask", "transform_params": {"mask_type": "name"}, "execution_order": 0},
        ])
        result = engine.apply({"name": "John Doe"})
        assert result["name"] == "J*** D***"

    def test_mask_full_redaction(self):
        engine = TransformEngine([
            {"source_field": "secret", "transform_type": "mask", "transform_params": {"mask_type": "full"}, "execution_order": 0},
        ])
        result = engine.apply({"secret": "my-api-key-12345"})
        assert result["secret"] == "***REDACTED***"

    def test_mask_partial(self):
        engine = TransformEngine([
            {"source_field": "card", "transform_type": "mask", "transform_params": {"mask_type": "partial"}, "execution_order": 0},
        ])
        result = engine.apply({"card": "4111111111111111"})
        masked = result["card"]
        assert masked.startswith("41")
        assert masked.endswith("11")
        assert "****" in masked

    def test_pii_masking_end_to_end(self):
        """Full PII masking pipeline: multiple sensitive fields in one pass."""
        engine = TransformEngine([
            {"source_field": "email", "transform_type": "mask", "transform_params": {"mask_type": "email"}, "execution_order": 0},
            {"source_field": "phone", "transform_type": "mask", "transform_params": {"mask_type": "phone"}, "execution_order": 0},
            {"source_field": "ssn", "transform_type": "mask", "transform_params": {"mask_type": "ssn"}, "execution_order": 0},
            {"source_field": "full_name", "transform_type": "mask", "transform_params": {"mask_type": "name"}, "execution_order": 0},
        ])
        input_data = {
            "order_id": "ORD-001",
            "email": "jane.smith@company.com",
            "phone": "555-987-6543",
            "ssn": "987-65-4321",
            "full_name": "Jane Smith",
            "amount": 150.00,
        }
        result = engine.apply(input_data)

        # Non-PII fields unchanged
        assert result["order_id"] == "ORD-001"
        assert result["amount"] == 150.00

        # PII fields masked
        assert "jane.smith" not in result["email"]
        assert result["phone"] == "***-***-6543"
        assert result["ssn"] == "***-**-4321"
        assert result["full_name"] == "J*** S***"


# ===========================================================================
# 3. QualityEngine
# ===========================================================================


class TestQualityEngineRules:
    def test_not_null_pass(self):
        engine = QualityEngine(
            rules=[{"id": "r1", "field_name": "status", "rule_type": "not_null", "severity": "reject", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"status": "ACTIVE"}, "key1", 1)
        assert result.passed is True
        assert result.violations == []

    def test_not_null_fail(self):
        engine = QualityEngine(
            rules=[{"id": "r1", "field_name": "status", "rule_type": "not_null", "severity": "reject", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"status": None}, "key1", 1)
        assert result.passed is False
        assert len(result.violations) == 1
        assert result.violations[0].violation_type == "not_null"
        assert result.violations[0].severity == "reject"
        assert result.should_reject is True

    def test_range_pass(self):
        engine = QualityEngine(
            rules=[{"id": "r2", "field_name": "amount", "rule_type": "range", "rule_params": {"min": 0, "max": 10000}, "severity": "warn", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"amount": 500}, "key1", 1)
        assert result.passed is True

    def test_range_below_min(self):
        engine = QualityEngine(
            rules=[{"id": "r2", "field_name": "amount", "rule_type": "range", "rule_params": {"min": 0, "max": 10000}, "severity": "warn", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"amount": -5}, "key1", 1)
        assert result.passed is False
        assert "below minimum" in result.violations[0].message

    def test_range_above_max(self):
        engine = QualityEngine(
            rules=[{"id": "r2", "field_name": "amount", "rule_type": "range", "rule_params": {"min": 0, "max": 10000}, "severity": "reject", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"amount": 99999}, "key1", 1)
        assert result.passed is False
        assert result.should_reject is True

    def test_regex_pass(self):
        engine = QualityEngine(
            rules=[{"id": "r3", "field_name": "code", "rule_type": "regex", "rule_params": {"pattern": r"^[A-Z]{2}-\d{4}$"}, "severity": "warn", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"code": "AB-1234"}, "key1", 1)
        assert result.passed is True

    def test_regex_fail(self):
        engine = QualityEngine(
            rules=[{"id": "r3", "field_name": "code", "rule_type": "regex", "rule_params": {"pattern": r"^[A-Z]{2}-\d{4}$"}, "severity": "warn", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"code": "invalid"}, "key1", 1)
        assert result.passed is False
        assert "pattern" in result.violations[0].message

    def test_enum_pass(self):
        engine = QualityEngine(
            rules=[{"id": "r4", "field_name": "status", "rule_type": "enum", "rule_params": {"values": ["NEW", "SHIPPED", "DELIVERED"]}, "severity": "warn", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"status": "NEW"}, "key1", 1)
        assert result.passed is True

    def test_enum_fail(self):
        engine = QualityEngine(
            rules=[{"id": "r4", "field_name": "status", "rule_type": "enum", "rule_params": {"values": ["NEW", "SHIPPED", "DELIVERED"]}, "severity": "reject", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"status": "INVALID"}, "key1", 1)
        assert result.passed is False
        assert "not in allowed set" in result.violations[0].message

    def test_length_pass(self):
        engine = QualityEngine(
            rules=[{"id": "r5", "field_name": "name", "rule_type": "length", "rule_params": {"min": 2, "max": 50}, "severity": "warn", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"name": "John"}, "key1", 1)
        assert result.passed is True

    def test_length_too_short(self):
        engine = QualityEngine(
            rules=[{"id": "r5", "field_name": "name", "rule_type": "length", "rule_params": {"min": 5, "max": 50}, "severity": "warn", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"name": "Jo"}, "key1", 1)
        assert result.passed is False
        assert "below minimum" in result.violations[0].message

    def test_length_too_long(self):
        engine = QualityEngine(
            rules=[{"id": "r5", "field_name": "name", "rule_type": "length", "rule_params": {"min": 1, "max": 5}, "severity": "warn", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"name": "Very Long Name"}, "key1", 1)
        assert result.passed is False
        assert "above maximum" in result.violations[0].message

    def test_null_value_skips_non_null_rules(self):
        """Rules other than not_null should skip null values."""
        engine = QualityEngine(
            rules=[{"id": "r1", "field_name": "amount", "rule_type": "range", "rule_params": {"min": 0, "max": 100}, "severity": "reject", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate({"amount": None}, "key1", 1)
        assert result.passed is True  # null skips range check

    def test_disabled_rule_skipped(self):
        engine = QualityEngine(
            rules=[{"id": "r1", "field_name": "status", "rule_type": "not_null", "severity": "reject", "is_enabled": False}],
            config_id="cfg-1",
        )
        result = engine.validate({"status": None}, "key1", 1)
        assert result.passed is True  # Rule disabled, no violation

    def test_has_rules_property(self):
        empty = QualityEngine(rules=[], config_id="cfg-1")
        assert empty.has_rules is False

        with_rules = QualityEngine(
            rules=[{"id": "r1", "field_name": "x", "rule_type": "not_null", "severity": "warn", "is_enabled": True}],
            config_id="cfg-1",
        )
        assert with_rules.has_rules is True

    def test_none_event_fields_passes(self):
        engine = QualityEngine(
            rules=[{"id": "r1", "field_name": "x", "rule_type": "not_null", "severity": "reject", "is_enabled": True}],
            config_id="cfg-1",
        )
        result = engine.validate(None, "key1", 1)
        assert result.passed is True


class TestQualityEngineMixed:
    """Test with multiple rules on multiple fields."""

    def _make_engine(self):
        return QualityEngine(
            rules=[
                {"id": "r1", "field_name": "amount", "rule_type": "not_null", "severity": "reject", "is_enabled": True},
                {"id": "r2", "field_name": "amount", "rule_type": "range", "rule_params": {"min": 0, "max": 1000000}, "severity": "warn", "is_enabled": True},
                {"id": "r3", "field_name": "status", "rule_type": "enum", "rule_params": {"values": ["NEW", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]}, "severity": "reject", "is_enabled": True},
                {"id": "r4", "field_name": "customer_id", "rule_type": "regex", "rule_params": {"pattern": r"^C-\d+"}, "severity": "warn", "is_enabled": True},
            ],
            config_id="cfg-mixed",
        )

    def test_all_pass(self):
        engine = self._make_engine()
        result = engine.validate(
            {"amount": 500, "status": "NEW", "customer_id": "C-12345"},
            "key1", 1,
        )
        assert result.passed is True

    def test_multiple_violations(self):
        engine = self._make_engine()
        result = engine.validate(
            {"amount": -10, "status": "INVALID", "customer_id": "BAD"},
            "key1", 1,
        )
        assert result.passed is False
        assert len(result.violations) >= 2
        violation_types = {v.violation_type for v in result.violations}
        assert "range" in violation_types
        assert "enum" in violation_types

    def test_reject_severity_triggers_should_reject(self):
        engine = self._make_engine()
        result = engine.validate(
            {"amount": None, "status": "NEW", "customer_id": "C-1"},
            "key1", 1,
        )
        assert result.should_reject is True

    def test_warn_only_does_not_reject(self):
        engine = self._make_engine()
        # customer_id fails regex (warn severity), but no reject-level violations
        result = engine.validate(
            {"amount": 100, "status": "NEW", "customer_id": "INVALID"},
            "key1", 1,
        )
        assert result.passed is False
        assert result.should_reject is False

    def test_violation_metadata(self):
        engine = self._make_engine()
        result = engine.validate(
            {"amount": None, "status": "NEW", "customer_id": "C-1"},
            "order-123", 42,
        )
        v = result.violations[0]
        assert v.config_id == "cfg-mixed"
        assert v.row_key == "order-123"
        assert v.event_seq == 42
        assert v.field_name == "amount"


# ===========================================================================
# 4. EventPipeline Integration
# ===========================================================================


class TestEventPipelineIntegration:
    """Test the EventPipeline with mock DB for rule/transform loading."""

    @pytest.fixture
    def mock_pipeline(self):
        """Create an EventPipeline with mocked DB connection."""
        from pipeline import EventPipeline

        pipeline = EventPipeline(PG_DSN, str(uuid.uuid4()))

        # Inject engines directly instead of loading from DB
        pipeline._quality = QualityEngine(
            rules=[
                {"id": "r1", "field_name": "amount", "rule_type": "not_null", "severity": "reject", "is_enabled": True},
                {"id": "r2", "field_name": "amount", "rule_type": "range", "rule_params": {"min": 0, "max": 100000}, "severity": "warn", "is_enabled": True},
                {"id": "r3", "field_name": "status", "rule_type": "enum", "rule_params": {"values": ["NEW", "SHIPPED"]}, "severity": "reject", "is_enabled": True},
            ],
            config_id=pipeline._config_id,
        )
        pipeline._transform = TransformEngine([
            {"source_field": "name", "transform_type": "upper", "transform_params": {}, "execution_order": 1},
            {"source_field": "email", "transform_type": "mask", "transform_params": {"mask_type": "email"}, "execution_order": 1},
        ])
        # Prevent automatic reload
        pipeline._last_reload = float("inf")

        return pipeline

    def test_process_valid_message(self, mock_pipeline):
        msg = make_cdc_message(
            fields={"amount": 500, "status": "NEW", "name": "john", "email": "john@test.com"},
        )
        # Mock persist_violations to avoid DB call
        mock_pipeline._persist_violations = MagicMock()

        result = mock_pipeline.process(msg)
        assert result is not None
        assert result.after["name"] == "JOHN"
        assert "john" not in result.after["email"]  # masked

    def test_process_rejected_message(self, mock_pipeline):
        msg = make_cdc_message(
            fields={"amount": None, "status": "NEW", "name": "test", "email": "a@b.com"},
        )
        mock_pipeline._persist_violations = MagicMock()

        result = mock_pipeline.process(msg)
        assert result is None  # Rejected due to not_null on amount

    def test_process_batch_filters_rejected(self, mock_pipeline):
        mock_pipeline._persist_violations = MagicMock()

        messages = [
            make_cdc_message(fields={"amount": 100, "status": "NEW", "name": "good", "email": "g@g.com"}, seq=1),
            make_cdc_message(fields={"amount": None, "status": "NEW", "name": "bad", "email": "b@b.com"}, seq=2),
            make_cdc_message(fields={"amount": 200, "status": "SHIPPED", "name": "ok", "email": "o@o.com"}, seq=3),
            make_cdc_message(fields={"amount": 50, "status": "INVALID", "name": "also bad", "email": "x@x.com"}, seq=4),
        ]

        results = mock_pipeline.process_batch(messages)
        assert len(results) == 2  # msg 1 and 3 pass; 2 (null amount) and 4 (bad status) rejected

    def test_process_batch_transforms_applied(self, mock_pipeline):
        mock_pipeline._persist_violations = MagicMock()

        messages = [
            make_cdc_message(fields={"amount": 100, "status": "NEW", "name": "alice", "email": "alice@co.com"}, seq=1),
        ]
        results = mock_pipeline.process_batch(messages)
        assert len(results) == 1
        assert results[0].after["name"] == "ALICE"

    def test_process_batch_violations_persisted(self, mock_pipeline):
        mock_pipeline._persist_violations = MagicMock()

        messages = [
            make_cdc_message(fields={"amount": None, "status": "NEW", "name": "x", "email": "x@x.com"}, seq=1),
        ]
        mock_pipeline.process_batch(messages)
        mock_pipeline._persist_violations.assert_called()
        # Check that violations were passed
        call_args = mock_pipeline._persist_violations.call_args[0][0]
        assert len(call_args) >= 1
        assert call_args[0].violation_type == "not_null"

    def test_process_batch_empty(self, mock_pipeline):
        results = mock_pipeline.process_batch([])
        assert results == []

    def test_process_delete_op_skips_quality(self, mock_pipeline):
        """DELETE events have after=None, should pass through quality checks."""
        mock_pipeline._persist_violations = MagicMock()

        msg = make_cdc_message(fields=None, op="D")
        result = mock_pipeline.process(msg)
        assert result is not None  # Deletes pass through

    def test_process_warn_severity_not_rejected(self, mock_pipeline):
        """Events with only warn-level violations should still be processed."""
        mock_pipeline._persist_violations = MagicMock()

        # amount=-1 violates range rule (warn severity) but not not_null (reject)
        msg = make_cdc_message(
            fields={"amount": -1, "status": "NEW", "name": "test", "email": "t@t.com"},
        )
        result = mock_pipeline.process(msg)
        # warn violations don't reject
        assert result is not None


# ===========================================================================
# 5. PII Masking End-to-End
# ===========================================================================


class TestPIIMaskingE2E:
    """Verify that PII data is never visible in output after pipeline processing."""

    def test_full_pii_pipeline(self):
        """Configure mask transforms for all PII fields and verify output."""
        engine = TransformEngine([
            {"source_field": "customer_email", "transform_type": "mask", "transform_params": {"mask_type": "email"}, "execution_order": 0},
            {"source_field": "customer_phone", "transform_type": "mask", "transform_params": {"mask_type": "phone"}, "execution_order": 0},
            {"source_field": "customer_ssn", "transform_type": "mask", "transform_params": {"mask_type": "ssn"}, "execution_order": 0},
            {"source_field": "customer_name", "transform_type": "mask", "transform_params": {"mask_type": "name"}, "execution_order": 0},
            {"source_field": "api_key", "transform_type": "mask", "transform_params": {"mask_type": "full"}, "execution_order": 0},
        ])

        input_data = {
            "order_id": "ORD-99999",
            "customer_email": "sensitive.user@bigcorp.com",
            "customer_phone": "(555) 867-5309",
            "customer_ssn": "078-05-1120",
            "customer_name": "Robert Tables",
            "api_key": "sk-live-abc123def456ghi789",
            "total_amount": 1250.00,
        }

        result = engine.apply(input_data)

        # Non-sensitive data unchanged
        assert result["order_id"] == "ORD-99999"
        assert result["total_amount"] == 1250.00

        # Sensitive data masked
        assert "sensitive.user" not in result["customer_email"]
        assert "@" in result["customer_email"]  # Still looks like email structure

        assert result["customer_phone"] == "***-***-5309"
        assert "555" not in result["customer_phone"]
        assert "867" not in result["customer_phone"]

        assert result["customer_ssn"] == "***-**-1120"
        assert "078" not in result["customer_ssn"]

        assert result["customer_name"] == "R*** T***"
        assert "Robert" not in result["customer_name"]

        assert result["api_key"] == "***REDACTED***"
        assert "sk-live" not in result["api_key"]

    def test_pii_in_serialized_output(self):
        """Verify PII is masked even when output is serialized to JSON."""
        engine = TransformEngine([
            {"source_field": "email", "transform_type": "mask", "transform_params": {"mask_type": "email"}, "execution_order": 0},
            {"source_field": "ssn", "transform_type": "mask", "transform_params": {"mask_type": "full"}, "execution_order": 0},
        ])
        result = engine.apply({"email": "victim@example.com", "ssn": "123-45-6789"})
        serialized = json.dumps(result)
        assert "victim" not in serialized
        assert "123-45-6789" not in serialized

    def test_mask_helper_functions_directly(self):
        assert mask_email("john.doe@example.com").startswith("j")
        assert "john.doe" not in mask_email("john.doe@example.com")

        assert mask_phone("(555) 123-4567") == "***-***-4567"

        assert mask_ssn("123-45-6789") == "***-**-6789"

        assert mask_name("John Doe") == "J*** D***"
        assert mask_name("Alice Bob Charlie") == "A*** B*** C***"


# ===========================================================================
# 6. Integration with real DB (violations persistence)
# ===========================================================================


class TestViolationsPersistence:
    """Test that violations are actually written to the database."""

    @pytest.fixture
    def pg_conn(self):
        try:
            conn = psycopg2.connect(PG_DSN)
            conn.autocommit = True
            yield conn
            conn.close()
        except psycopg2.OperationalError:
            pytest.skip("Postgres not available")

    def test_pipeline_persists_violations_to_db(self, pg_conn):
        """End-to-end: process a bad event and verify violation row in DB."""
        from pipeline import EventPipeline

        # We need a valid config_id that exists in the DB
        cursor = pg_conn.cursor()

        # Create connector + replication config for FK
        conn_id = str(uuid.uuid4())
        config_id = str(uuid.uuid4())
        rule_id = str(uuid.uuid4())

        cursor.execute(
            "INSERT INTO connector_configs (id, type, name, credentials_encrypted) "
            "VALUES (%s, %s, %s, %s)",
            (conn_id, "snowflake", "viol-test-conn", "enc"),
        )
        cursor.execute(
            "INSERT INTO replication_configs "
            "(id, source_table_schema, source_table_name, connector_config_id, "
            "target_table_name, field_mappings, primary_key_fields) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (config_id, "SAPSR3", "TEST_TABLE", conn_id, "test_target", "[]", '["ID"]'),
        )
        cursor.execute(
            "INSERT INTO data_quality_rules (id, config_id, field_name, rule_type, "
            "rule_params, severity, is_enabled) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (rule_id, config_id, "amount", "not_null", "{}", "reject", True),
        )

        try:
            pipeline = EventPipeline(PG_DSN, config_id)
            # Force reload
            pipeline._last_reload = 0
            pipeline.reload_if_needed()

            # Process a message that will violate the not_null rule
            msg = make_cdc_message(
                fields={"amount": None, "status": "NEW"},
                key={"ID": "test-key"},
            )
            result = pipeline.process(msg)
            assert result is None  # Rejected

            # Check violations table
            cursor.execute(
                "SELECT field_name, violation_type, severity, message "
                "FROM data_quality_violations WHERE config_id = %s",
                (config_id,),
            )
            rows = cursor.fetchall()
            assert len(rows) >= 1
            assert rows[0][0] == "amount"
            assert rows[0][1] == "not_null"
            assert rows[0][2] == "reject"

        finally:
            # Cleanup
            cursor.execute(
                "DELETE FROM data_quality_violations WHERE config_id = %s", (config_id,)
            )
            cursor.execute(
                "DELETE FROM data_quality_rules WHERE config_id = %s", (config_id,)
            )
            cursor.execute(
                "DELETE FROM replication_configs WHERE id = %s", (config_id,)
            )
            cursor.execute(
                "DELETE FROM connector_configs WHERE id = %s", (conn_id,)
            )
