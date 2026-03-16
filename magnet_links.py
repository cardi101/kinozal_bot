import os
import re
from typing import Any, Dict
from urllib.parse import quote

def _compact(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()

def build_raw_magnet(item: Dict[str, Any]) -> str:
    info_hash = _compact(item.get("source_info_hash") or "").upper()
    if not info_hash:
        return ""
    title = _compact(item.get("source_title") or "kinozal")
    return f"magnet:?xt=urn:btih:{info_hash}&dn={quote(title)}"

def build_public_magnet_redirect_url(item: Dict[str, Any]) -> str:
    base = _compact(os.getenv("MAGNET_BASE_URL", "")).rstrip("/")
    info_hash = _compact(item.get("source_info_hash") or "").upper()
    if not base or not info_hash:
        return ""
    title = _compact(item.get("source_title") or "kinozal")
    return f"{base}/m/{info_hash}?dn={quote(title)}"
