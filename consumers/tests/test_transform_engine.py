import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transform_engine import TransformEngine, mask_email, mask_phone, mask_ssn, mask_name


def test_upper_transform():
    engine = TransformEngine([{"source_field": "STATUS", "transform_type": "upper", "transform_params": {}, "execution_order": 0}])
    result = engine.apply({"STATUS": "shipped", "ID": "1"})
    assert result["STATUS"] == "SHIPPED"
    assert result["ID"] == "1"  # Untouched


def test_lower_transform():
    engine = TransformEngine([{"source_field": "NAME", "transform_type": "lower", "transform_params": {}, "execution_order": 0}])
    assert engine.apply({"NAME": "JOHN DOE"})["NAME"] == "john doe"


def test_trim_transform():
    engine = TransformEngine([{"source_field": "VAL", "transform_type": "trim", "transform_params": {}, "execution_order": 0}])
    assert engine.apply({"VAL": "  hello  "})["VAL"] == "hello"


def test_cast_int():
    engine = TransformEngine([{"source_field": "AMT", "transform_type": "cast", "transform_params": {"type": "int"}, "execution_order": 0}])
    assert engine.apply({"AMT": "42.7"})["AMT"] == 42


def test_cast_float():
    engine = TransformEngine([{"source_field": "AMT", "transform_type": "cast", "transform_params": {"type": "float"}, "execution_order": 0}])
    assert engine.apply({"AMT": "42"})["AMT"] == 42.0


def test_hash_sha256():
    engine = TransformEngine([{"source_field": "SSN", "transform_type": "hash", "transform_params": {"algorithm": "sha256"}, "execution_order": 0}])
    result = engine.apply({"SSN": "123-45-6789"})
    assert len(result["SSN"]) == 64  # SHA-256 hex length
    assert result["SSN"] != "123-45-6789"


def test_mask_full():
    engine = TransformEngine([{"source_field": "SECRET", "transform_type": "mask", "transform_params": {"mask_type": "full"}, "execution_order": 0}])
    assert engine.apply({"SECRET": "my_secret"})["SECRET"] == "***REDACTED***"


def test_mask_partial():
    engine = TransformEngine([{"source_field": "CARD", "transform_type": "mask", "transform_params": {"mask_type": "partial"}, "execution_order": 0}])
    result = engine.apply({"CARD": "4111222233334444"})
    assert result["CARD"].startswith("41")
    assert result["CARD"].endswith("44")
    assert "****" in result["CARD"]


def test_default_transform():
    engine = TransformEngine([{"source_field": "STATUS", "transform_type": "default", "transform_params": {"value": "UNKNOWN"}, "execution_order": 0}])
    assert engine.apply({"STATUS": None})["STATUS"] == "UNKNOWN"
    assert engine.apply({"STATUS": "ACTIVE"})["STATUS"] == "ACTIVE"


def test_chained_transforms():
    """Multiple transforms on same field execute in order."""
    engine = TransformEngine([
        {"source_field": "NAME", "transform_type": "trim", "transform_params": {}, "execution_order": 0},
        {"source_field": "NAME", "transform_type": "upper", "transform_params": {}, "execution_order": 1},
    ])
    assert engine.apply({"NAME": "  john doe  "})["NAME"] == "JOHN DOE"


def test_null_passthrough():
    engine = TransformEngine([{"source_field": "VAL", "transform_type": "upper", "transform_params": {}, "execution_order": 0}])
    assert engine.apply({"VAL": None})["VAL"] is None


def test_none_fields_passthrough():
    engine = TransformEngine([])
    assert engine.apply(None) is None


def test_mask_email():
    assert "@" in mask_email("john.doe@example.com")
    assert "john" not in mask_email("john.doe@example.com")


def test_mask_phone():
    result = mask_phone("(555) 123-4567")
    assert result.endswith("4567")
    assert "555" not in result


def test_mask_ssn():
    result = mask_ssn("123-45-6789")
    assert result.endswith("6789")
    assert "123" not in result


def test_mask_name():
    result = mask_name("John Doe")
    assert result.startswith("J")
    assert "ohn" not in result
