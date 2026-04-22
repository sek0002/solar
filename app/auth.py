from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from typing import Any


def hash_password(password: str, *, salt: bytes | None = None) -> str:
    salt_bytes = salt or secrets.token_bytes(16)
    derived = hashlib.scrypt(password.encode("utf-8"), salt=salt_bytes, n=2**14, r=8, p=1)
    return "scrypt${}${}".format(
        base64.urlsafe_b64encode(salt_bytes).decode("ascii").rstrip("="),
        base64.urlsafe_b64encode(derived).decode("ascii").rstrip("="),
    )


def verify_password(password: str, password_hash: str) -> bool:
    try:
        algorithm, salt_token, derived_token = password_hash.split("$", 2)
    except ValueError:
        return False
    if algorithm != "scrypt":
        return False
    salt_bytes = _urlsafe_b64decode(salt_token)
    expected = _urlsafe_b64decode(derived_token)
    actual = hashlib.scrypt(password.encode("utf-8"), salt=salt_bytes, n=2**14, r=8, p=1)
    return hmac.compare_digest(actual, expected)


def create_signed_token(payload: dict[str, Any], secret: str) -> str:
    body = base64.urlsafe_b64encode(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).decode("ascii").rstrip("=")
    signature = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{body}.{signature}"


def verify_signed_token(token: str, secret: str) -> dict[str, Any] | None:
    if "." not in token:
        return None
    body, signature = token.rsplit(".", 1)
    expected = hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        return None
    try:
        payload = json.loads(_urlsafe_b64decode(body).decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    expires_at = payload.get("exp")
    if not isinstance(expires_at, (int, float)) or time.time() > float(expires_at):
        return None
    return payload


def verify_totp(secret: str, code: str, *, step_seconds: int = 30, digits: int = 6, window: int = 1) -> bool:
    normalized = "".join(code.split())
    if not normalized.isdigit() or len(normalized) != digits:
        return False
    secret_bytes = base64.b32decode(_normalize_totp_secret(secret), casefold=True)
    counter = int(time.time() // step_seconds)
    for offset in range(-window, window + 1):
        if hmac.compare_digest(normalized, generate_totp(secret_bytes, counter + offset, digits=digits)):
            return True
    return False


def generate_totp(secret_bytes: bytes, counter: int, *, digits: int = 6) -> str:
    message = counter.to_bytes(8, "big")
    digest = hmac.new(secret_bytes, message, hashlib.sha1).digest()
    offset = digest[-1] & 0x0F
    truncated = int.from_bytes(digest[offset:offset + 4], "big") & 0x7FFFFFFF
    return str(truncated % (10**digits)).zfill(digits)


def _normalize_totp_secret(secret: str) -> str:
    normalized = "".join(secret.strip().split()).upper()
    padding = (-len(normalized)) % 8
    return normalized + ("=" * padding)


def _urlsafe_b64decode(value: str) -> bytes:
    padded = value + ("=" * ((4 - len(value) % 4) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))
