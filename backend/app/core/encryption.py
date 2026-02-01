"""Encryption utilities for sensitive data like API keys."""

import base64
import os
from functools import lru_cache

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from app.config import get_settings


class EncryptionManager:
    """
    Manager for encrypting/decrypting sensitive data.
    
    Uses Fernet symmetric encryption with a key derived from SECRET_KEY.
    All API keys (WB, Ozon, etc.) should be encrypted before storing in DB.
    """

    def __init__(self, secret_key: str):
        """Initialize encryption with derived key from secret."""
        # Use PBKDF2 to derive a proper Fernet key from the secret
        salt = b"mms_api_key_salt"  # Static salt (can be made dynamic per-user)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=480000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(secret_key.encode()))
        self._fernet = Fernet(key)

    def encrypt(self, plaintext: str) -> bytes:
        """
        Encrypt a string and return encrypted bytes.
        
        Args:
            plaintext: The string to encrypt (e.g., API key)
            
        Returns:
            Encrypted bytes suitable for storing in DB as BYTEA
        """
        if not plaintext:
            return b""
        return self._fernet.encrypt(plaintext.encode("utf-8"))

    def decrypt(self, ciphertext: bytes) -> str:
        """
        Decrypt bytes and return the original string.
        
        Args:
            ciphertext: Encrypted bytes from DB
            
        Returns:
            Original plaintext string
        """
        if not ciphertext:
            return ""
        return self._fernet.decrypt(ciphertext).decode("utf-8")

    def encrypt_to_string(self, plaintext: str) -> str:
        """
        Encrypt and return as base64 string (for JSON storage).
        
        Args:
            plaintext: The string to encrypt
            
        Returns:
            Base64-encoded encrypted string
        """
        if not plaintext:
            return ""
        encrypted = self.encrypt(plaintext)
        return base64.urlsafe_b64encode(encrypted).decode("utf-8")

    def decrypt_from_string(self, ciphertext_b64: str) -> str:
        """
        Decrypt from base64 string.
        
        Args:
            ciphertext_b64: Base64-encoded encrypted string
            
        Returns:
            Original plaintext string
        """
        if not ciphertext_b64:
            return ""
        ciphertext = base64.urlsafe_b64decode(ciphertext_b64.encode("utf-8"))
        return self.decrypt(ciphertext)


@lru_cache
def get_encryption_manager() -> EncryptionManager:
    """Get cached encryption manager instance."""
    settings = get_settings()
    return EncryptionManager(settings.secret_key)


# Convenience functions
def encrypt_api_key(api_key: str) -> bytes:
    """Encrypt an API key for database storage."""
    return get_encryption_manager().encrypt(api_key)


def decrypt_api_key(encrypted_key: bytes) -> str:
    """Decrypt an API key from database."""
    return get_encryption_manager().decrypt(encrypted_key)
