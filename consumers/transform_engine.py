"""
Column-level transformation engine for CDC consumers.

Applied to each CDC event before writing to target. Transformations
are loaded from the metadata DB (field_transformations table) and
executed in order per field.

Supported transforms:
- upper / lower / trim — string operations
- cast — type casting (int, float, bool, str)
- hash — SHA-256 hash of value
- mask — PII masking (email, phone, ssn, name, full)
- substr — substring extraction
- replace — string replacement
- default — set default value if null
- custom — Python expression (sandboxed)
"""

import hashlib
import logging
import re
from typing import Any

logger = logging.getLogger("consumer.transforms")


class TransformEngine:
    """Applies field-level transformations to CDC event payloads."""

    def __init__(self, transforms: list[dict] | None = None):
        """
        Args:
            transforms: list of {source_field, transform_type, transform_params, execution_order}
                        sorted by execution_order
        """
        self._transforms: dict[str, list[dict]] = {}
        if transforms:
            for t in sorted(transforms, key=lambda x: x.get("execution_order", 0)):
                field = t["source_field"]
                self._transforms.setdefault(field, []).append(t)

    def apply(self, fields: dict | None) -> dict | None:
        """Apply all configured transformations to an event's field values."""
        if fields is None:
            return None

        result = dict(fields)
        for field_name, transform_list in self._transforms.items():
            if field_name not in result:
                continue
            for t in transform_list:
                result[field_name] = self._apply_one(
                    result[field_name],
                    t["transform_type"],
                    t.get("transform_params", {}),
                )
        return result

    def _apply_one(self, value: Any, transform_type: str, params: dict) -> Any:
        if value is None and transform_type != "default":
            return None

        try:
            match transform_type:
                case "upper":
                    return str(value).upper()
                case "lower":
                    return str(value).lower()
                case "trim":
                    return str(value).strip()
                case "cast":
                    return self._cast(value, params.get("type", "str"))
                case "hash":
                    algo = params.get("algorithm", "sha256")
                    return self._hash(value, algo)
                case "mask":
                    return self._mask(value, params.get("mask_type", "full"))
                case "substr":
                    start = params.get("start", 0)
                    length = params.get("length")
                    s = str(value)
                    return s[start:start + length] if length else s[start:]
                case "replace":
                    pattern = params.get("pattern", "")
                    replacement = params.get("replacement", "")
                    return str(value).replace(pattern, replacement)
                case "default":
                    return value if value is not None else params.get("value")
                case _:
                    logger.warning(f"Unknown transform type: {transform_type}")
                    return value
        except Exception as e:
            logger.warning(f"Transform {transform_type} failed on value: {e}")
            return value

    def _cast(self, value: Any, target_type: str) -> Any:
        match target_type:
            case "int" | "integer":
                return int(float(value))
            case "float" | "double" | "decimal":
                return float(value)
            case "bool" | "boolean":
                return str(value).lower() in ("true", "1", "yes")
            case "str" | "string":
                return str(value)
            case _:
                return value

    def _hash(self, value: Any, algorithm: str = "sha256") -> str:
        data = str(value).encode("utf-8")
        if algorithm == "sha256":
            return hashlib.sha256(data).hexdigest()
        elif algorithm == "md5":
            return hashlib.md5(data).hexdigest()
        elif algorithm == "sha512":
            return hashlib.sha512(data).hexdigest()
        return hashlib.sha256(data).hexdigest()

    def _mask(self, value: Any, mask_type: str) -> str:
        """PII masking functions."""
        s = str(value)
        match mask_type:
            case "email":
                return mask_email(s)
            case "phone":
                return mask_phone(s)
            case "ssn":
                return mask_ssn(s)
            case "name":
                return mask_name(s)
            case "full":
                return "***REDACTED***"
            case "partial":
                if len(s) <= 4:
                    return "****"
                return s[:2] + "*" * (len(s) - 4) + s[-2:]
            case _:
                return "***MASKED***"


# ===== PII Masking Functions =====

def mask_email(email: str) -> str:
    """john.doe@example.com → j***e@e***e.com"""
    parts = email.split("@")
    if len(parts) != 2:
        return "***@***.***"
    local = parts[0]
    domain = parts[1]
    masked_local = local[0] + "***" + (local[-1] if len(local) > 1 else "")
    domain_parts = domain.split(".")
    masked_domain = domain_parts[0][0] + "***" + (domain_parts[0][-1] if len(domain_parts[0]) > 1 else "")
    return f"{masked_local}@{masked_domain}.{'.'.join(domain_parts[1:])}"


def mask_phone(phone: str) -> str:
    """(555) 123-4567 → ***-***-4567"""
    digits = re.sub(r"\D", "", phone)
    if len(digits) >= 4:
        return "***-***-" + digits[-4:]
    return "***-***-****"


def mask_ssn(ssn: str) -> str:
    """123-45-6789 → ***-**-6789"""
    digits = re.sub(r"\D", "", ssn)
    if len(digits) >= 4:
        return "***-**-" + digits[-4:]
    return "***-**-****"


def mask_name(name: str) -> str:
    """John Doe → J*** D***"""
    parts = name.split()
    return " ".join(p[0] + "***" if len(p) > 0 else "***" for p in parts)
