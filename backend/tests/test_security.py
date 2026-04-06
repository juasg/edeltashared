"""Tests for AES-256-GCM credential encryption."""

import json
import os
from unittest.mock import patch

import pytest


def test_encrypt_decrypt_roundtrip():
    """Encrypted credentials should decrypt to the original value."""
    with patch("app.core.security.settings") as mock_settings:
        mock_settings.encryption_key = "test_key_that_is_32_bytes_long!!"
        from app.core.security import decrypt_credentials, encrypt_credentials

        original = json.dumps({
            "account": "my_account",
            "user": "admin",
            "password": "s3cret!",
        })

        encrypted = encrypt_credentials(original)
        decrypted = decrypt_credentials(encrypted)

        assert decrypted == original
        assert encrypted != original  # Should not be plaintext


def test_different_encryptions_differ():
    """Two encryptions of the same value should produce different ciphertexts (random nonce)."""
    with patch("app.core.security.settings") as mock_settings:
        mock_settings.encryption_key = "test_key_that_is_32_bytes_long!!"
        from app.core.security import encrypt_credentials

        value = "same_secret_value"
        enc1 = encrypt_credentials(value)
        enc2 = encrypt_credentials(value)

        assert enc1 != enc2  # Different nonces


def test_wrong_key_fails():
    """Decrypting with wrong key should raise an error."""
    with patch("app.core.security.settings") as mock_settings:
        mock_settings.encryption_key = "key_one_that_is_32_bytes_long!!!"
        from app.core.security import encrypt_credentials

        encrypted = encrypt_credentials("secret")

    with patch("app.core.security.settings") as mock_settings:
        mock_settings.encryption_key = "key_two_that_is_32_bytes_long!!!"
        from app.core.security import decrypt_credentials

        with pytest.raises(Exception):
            decrypt_credentials(encrypted)
