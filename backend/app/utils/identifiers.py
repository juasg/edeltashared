"""
SQL identifier validation — prevents SQL injection through table/schema/column names.

All user-supplied identifiers MUST pass through validate_identifier() before
being interpolated into SQL strings (DDL templates, dynamic queries, etc).
"""

import re

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,254}$")


def validate_identifier(name: str, label: str = "identifier") -> str:
    """Validate that a string is a safe SQL identifier.

    Raises ValueError if the name contains characters that could
    enable SQL injection when interpolated into DDL/DML.
    """
    if not name:
        raise ValueError(f"{label} cannot be empty")
    if not _IDENTIFIER_PATTERN.match(name):
        raise ValueError(
            f"Invalid {label}: '{name}'. "
            f"Must start with a letter/underscore and contain only alphanumeric/underscore characters."
        )
    return name


def safe_quote(name: str, label: str = "identifier") -> str:
    """Validate and double-quote an identifier for use in SQL."""
    validate_identifier(name, label)
    return f'"{name}"'
