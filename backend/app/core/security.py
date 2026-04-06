import base64
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from cryptography.hazmat.primitives import hashes

from app.core.config import settings


def _get_key() -> bytes:
    """Derive a 32-byte AES key using HKDF from the configured encryption key."""
    raw = settings.encryption_key.encode("utf-8")
    hkdf = HKDF(
        algorithm=hashes.SHA256(),
        length=32,
        salt=None,
        info=b"edeltashared-credential-encryption",
    )
    return hkdf.derive(raw)


def encrypt_credentials(plaintext: str) -> str:
    """Encrypt a string using AES-256-GCM. Returns base64-encoded nonce+ciphertext."""
    key = _get_key()
    aesgcm = AESGCM(key)
    nonce = os.urandom(12)
    ciphertext = aesgcm.encrypt(nonce, plaintext.encode("utf-8"), None)
    return base64.b64encode(nonce + ciphertext).decode("utf-8")


def decrypt_credentials(encrypted: str) -> str:
    """Decrypt a base64-encoded AES-256-GCM encrypted string."""
    key = _get_key()
    aesgcm = AESGCM(key)
    raw = base64.b64decode(encrypted)
    nonce = raw[:12]
    ciphertext = raw[12:]
    plaintext = aesgcm.decrypt(nonce, ciphertext, None)
    return plaintext.decode("utf-8")
