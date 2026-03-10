import hashlib
import html
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Optional


def utc_ts() -> int:
    return int(time.time())


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        return datetime.fromisoformat(value)
    except Exception:
        pass
    try:
        return parsedate_to_datetime(value)
    except Exception:
        return None


def compact_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def strip_html(text: str) -> str:
    text = html.unescape(text or "")
    text = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return compact_spaces(text)


def short(text: str, limit: int) -> str:
    text = text or ""
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def md5_text(text: str) -> str:
    return hashlib.md5(text.encode("utf-8", errors="ignore")).hexdigest()


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()
