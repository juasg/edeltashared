import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from quality_engine import QualityEngine


def _make_rules(rules_data):
    return [{"id": f"rule-{i}", **r} for i, r in enumerate(rules_data)]


def test_not_null_pass():
    rules = _make_rules([{"field_name": "ORDER_ID", "rule_type": "not_null", "rule_params": {}, "severity": "reject"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"ORDER_ID": "123"}, "123", 1)
    assert result.passed
    assert not result.should_reject


def test_not_null_fail():
    rules = _make_rules([{"field_name": "ORDER_ID", "rule_type": "not_null", "rule_params": {}, "severity": "reject"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"ORDER_ID": None}, "123", 1)
    assert not result.passed
    assert result.should_reject
    assert len(result.violations) == 1
    assert "null" in result.violations[0].message.lower()


def test_range_pass():
    rules = _make_rules([{"field_name": "AMOUNT", "rule_type": "range", "rule_params": {"min": 0, "max": 10000}, "severity": "warn"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"AMOUNT": 500}, "k1", 1)
    assert result.passed


def test_range_below_min():
    rules = _make_rules([{"field_name": "AMOUNT", "rule_type": "range", "rule_params": {"min": 0, "max": 10000}, "severity": "warn"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"AMOUNT": -5}, "k1", 1)
    assert not result.passed
    assert "below minimum" in result.violations[0].message


def test_range_above_max():
    rules = _make_rules([{"field_name": "AMOUNT", "rule_type": "range", "rule_params": {"min": 0, "max": 10000}, "severity": "reject"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"AMOUNT": 99999}, "k1", 1)
    assert not result.passed
    assert result.should_reject


def test_regex_pass():
    rules = _make_rules([{"field_name": "ORDER_ID", "rule_type": "regex", "rule_params": {"pattern": "^ORD-\\d+"}, "severity": "warn"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"ORDER_ID": "ORD-12345"}, "k1", 1)
    assert result.passed


def test_regex_fail():
    rules = _make_rules([{"field_name": "ORDER_ID", "rule_type": "regex", "rule_params": {"pattern": "^ORD-\\d+"}, "severity": "warn"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"ORDER_ID": "INVALID-123"}, "k1", 1)
    assert not result.passed
    assert "pattern" in result.violations[0].message


def test_enum_pass():
    rules = _make_rules([{"field_name": "STATUS", "rule_type": "enum", "rule_params": {"values": ["NEW", "SHIPPED", "DELIVERED"]}, "severity": "warn"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"STATUS": "SHIPPED"}, "k1", 1)
    assert result.passed


def test_enum_fail():
    rules = _make_rules([{"field_name": "STATUS", "rule_type": "enum", "rule_params": {"values": ["NEW", "SHIPPED"]}, "severity": "warn"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"STATUS": "INVALID"}, "k1", 1)
    assert not result.passed


def test_length_pass():
    rules = _make_rules([{"field_name": "CODE", "rule_type": "length", "rule_params": {"min": 2, "max": 10}, "severity": "warn"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"CODE": "ABC"}, "k1", 1)
    assert result.passed


def test_length_too_long():
    rules = _make_rules([{"field_name": "CODE", "rule_type": "length", "rule_params": {"max": 5}, "severity": "reject"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"CODE": "TOOLONGSTRING"}, "k1", 1)
    assert not result.passed
    assert result.should_reject


def test_multiple_rules_on_same_field():
    rules = _make_rules([
        {"field_name": "AMOUNT", "rule_type": "not_null", "rule_params": {}, "severity": "reject"},
        {"field_name": "AMOUNT", "rule_type": "range", "rule_params": {"min": 0}, "severity": "warn"},
    ])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"AMOUNT": None}, "k1", 1)
    # not_null fires, range skips (null value)
    assert len(result.violations) == 1
    assert result.should_reject


def test_warn_does_not_reject():
    rules = _make_rules([{"field_name": "X", "rule_type": "range", "rule_params": {"max": 10}, "severity": "warn"}])
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"X": 999}, "k1", 1)
    assert not result.passed
    assert not result.should_reject  # Warn only


def test_no_rules_passes():
    engine = QualityEngine([], "cfg-1")
    result = engine.validate({"anything": "goes"}, "k1", 1)
    assert result.passed


def test_disabled_rule_skipped():
    rules = [{"id": "r1", "field_name": "X", "rule_type": "not_null", "rule_params": {}, "severity": "reject", "is_enabled": False}]
    engine = QualityEngine(rules, "cfg-1")
    result = engine.validate({"X": None}, "k1", 1)
    assert result.passed  # Rule disabled, so passes
