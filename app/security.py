import hashlib
import hmac
from datetime import datetime, timezone
from typing import Tuple

from fastapi import Header, HTTPException

from .config import settings


def verify_hmac_headers(raw_body: bytes, x_timestamp: str, x_signature: str) -> None:
    # Timestamp validation
    try:
        ts = datetime.fromisoformat(x_timestamp.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid X-Timestamp")

    now = datetime.now(timezone.utc)
    skew = abs((now - ts).total_seconds())
    if skew > settings.webhook_max_skew_seconds:
        raise HTTPException(status_code=401, detail="Timestamp skew too large")

    # Signature validation
    expected = hmac.new(
        key=settings.webhook_hmac_secret.encode("utf-8"),
        msg=f"{x_timestamp}.{raw_body.decode('utf-8')}".encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()

    if not x_signature.startswith("sha256="):
        raise HTTPException(status_code=400, detail="Invalid X-Signature format")

    provided = x_signature.split("=", 1)[1]
    if not hmac.compare_digest(expected, provided):
        raise HTTPException(status_code=401, detail="Signature mismatch")


