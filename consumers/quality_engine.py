"""
Data quality validation engine for CDC consumers.

Validates each CDC event against configured rules before applying
to the target. Rules are loaded from the metadata DB (data_quality_rules table).

Actions per severity:
- warn: log violation, apply event anyway
- reject: skip event, log violation
- quarantine: write to quarantine table, skip target apply

Supported rule types:
- not_null — field must not be null
- range — numeric value within min/max
- regex — value matches pattern
- enum — value in allowed set
- length — string length within bounds
- custom_sql — evaluate a SQL expression (future)
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger("consumer.quality")


@dataclass
class Violation:
    rule_id: str
    config_id: str
    field_name: str
    field_value: str | None
    row_key: str
    violation_type: str
    severity: str
    message: str
    event_seq: int


@dataclass
class ValidationResult:
    passed: bool = True
    violations: list[Violation] = field(default_factory=list)
    should_reject: bool = False


class QualityEngine:
    """Validates CDC events against data quality rules."""

    def __init__(self, rules: list[dict] | None = None, config_id: str = ""):
        """
        Args:
            rules: list of {id, field_name, rule_type, rule_params, severity, is_enabled}
            config_id: replication config UUID
        """
        self._rules: dict[str, list[dict]] = {}
        self._config_id = config_id

        if rules:
            for rule in rules:
                if not rule.get("is_enabled", True):
                    continue
                field_name = rule["field_name"]
                self._rules.setdefault(field_name, []).append(rule)

    @property
    def has_rules(self) -> bool:
        return len(self._rules) > 0

    def validate(self, event_fields: dict | None, row_key: str, event_seq: int) -> ValidationResult:
        """Validate an event's fields against all applicable rules."""
        result = ValidationResult()

        if event_fields is None:
            return result

        for field_name, rules in self._rules.items():
            value = event_fields.get(field_name)

            for rule in rules:
                violation = self._check_rule(rule, field_name, value, row_key, event_seq)
                if violation:
                    result.violations.append(violation)
                    result.passed = False
                    if violation.severity == "reject":
                        result.should_reject = True

        return result

    def _check_rule(
        self, rule: dict, field_name: str, value: Any, row_key: str, event_seq: int
    ) -> Violation | None:
        rule_type = rule["rule_type"]
        params = rule.get("rule_params", {})
        severity = rule.get("severity", "warn")

        try:
            match rule_type:
                case "not_null":
                    if value is None:
                        return self._violation(
                            rule, field_name, value, row_key, event_seq,
                            f"Field '{field_name}' is null"
                        )

                case "range":
                    if value is not None:
                        num_val = float(value)
                        min_val = params.get("min")
                        max_val = params.get("max")
                        if min_val is not None and num_val < float(min_val):
                            return self._violation(
                                rule, field_name, value, row_key, event_seq,
                                f"Field '{field_name}' value {value} below minimum {min_val}"
                            )
                        if max_val is not None and num_val > float(max_val):
                            return self._violation(
                                rule, field_name, value, row_key, event_seq,
                                f"Field '{field_name}' value {value} above maximum {max_val}"
                            )

                case "regex":
                    if value is not None:
                        pattern = params.get("pattern", "")
                        if not re.match(pattern, str(value)):
                            return self._violation(
                                rule, field_name, value, row_key, event_seq,
                                f"Field '{field_name}' doesn't match pattern '{pattern}'"
                            )

                case "enum":
                    if value is not None:
                        allowed = params.get("values", [])
                        if str(value) not in [str(v) for v in allowed]:
                            return self._violation(
                                rule, field_name, value, row_key, event_seq,
                                f"Field '{field_name}' value '{value}' not in allowed set"
                            )

                case "length":
                    if value is not None:
                        str_len = len(str(value))
                        min_len = params.get("min")
                        max_len = params.get("max")
                        if min_len is not None and str_len < int(min_len):
                            return self._violation(
                                rule, field_name, value, row_key, event_seq,
                                f"Field '{field_name}' length {str_len} below minimum {min_len}"
                            )
                        if max_len is not None and str_len > int(max_len):
                            return self._violation(
                                rule, field_name, value, row_key, event_seq,
                                f"Field '{field_name}' length {str_len} above maximum {max_len}"
                            )
                case _:
                    pass

        except (ValueError, TypeError) as e:
            logger.warning(f"Rule check failed for {field_name}: {e}")

        return None

    def _violation(
        self, rule: dict, field_name: str, value: Any, row_key: str, event_seq: int, message: str
    ) -> Violation:
        return Violation(
            rule_id=rule.get("id", ""),
            config_id=self._config_id,
            field_name=field_name,
            field_value=str(value) if value is not None else None,
            row_key=row_key,
            violation_type=rule["rule_type"],
            severity=rule.get("severity", "warn"),
            message=message,
            event_seq=event_seq,
        )
