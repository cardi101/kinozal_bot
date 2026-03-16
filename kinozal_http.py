import asyncio
import logging
import os
import re
from html import unescape
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urljoin

import httpx

log = logging.getLogger("kinozal-http")

KINOZAL_BASE = "https://kinozal.tv"
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

_client: Optional[httpx.AsyncClient] = None
_login_lock = asyncio.Lock()
_login_attempted = False
_login_ok = False


def _compact(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _decode_html_bytes(raw: bytes) -> str:
    for enc in ("cp1251", "windows-1251", "utf-8"):
        try:
            return raw.decode(enc)
        except Exception:
            pass
    return raw.decode("utf-8", errors="replace")


def _extract_attr(attrs: str, name: str) -> str:
    m = re.search(rf'{name}\s*=\s*["\']([^"\']*)["\']', attrs, flags=re.I)
    if m:
        return unescape(m.group(1))
    m = re.search(rf"{name}\s*=\s*([^\s>]+)", attrs, flags=re.I)
    if m:
        return unescape(m.group(1))
    return ""


def _parse_login_form(html: str) -> Tuple[str, Dict[str, str], str, str]:
    form_match = re.search(
        r'(?is)<form[^>]+action=["\']([^"\']*takelogin\.php[^"\']*)["\'][^>]*>(.*?)</form>',
        html,
    )
    if not form_match:
        return "", {}, "", ""

    action = urljoin(KINOZAL_BASE, unescape(form_match.group(1)))
    form_html = form_match.group(2)

    inputs: Dict[str, str] = {}
    username_field = ""
    password_field = ""

    for input_attrs in re.findall(r"(?is)<input\b([^>]*)>", form_html):
        name = _extract_attr(input_attrs, "name")
        if not name:
            continue
        value = _extract_attr(input_attrs, "value")
        input_type = (_extract_attr(input_attrs, "type") or "text").strip().lower()

        inputs[name] = value

        if input_type == "password" and not password_field:
            password_field = name
        elif input_type in {"text", "email"} and not username_field:
            username_field = name

        lowered = name.lower()
        if lowered in {"username", "login", "nick", "user", "email"}:
            username_field = name
        if lowered in {"password", "pass", "passwd"}:
            password_field = name

    return action, inputs, username_field, password_field


async def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            timeout=60,
            follow_redirects=True,
            headers={"User-Agent": _USER_AGENT},
        )
    return _client


async def _ensure_login() -> httpx.AsyncClient:
    global _login_attempted, _login_ok

    client = await _get_client()
    username = os.getenv("KINOZAL_USERNAME", "").strip()
    password = os.getenv("KINOZAL_PASSWORD", "").strip()

    if not username or not password:
        return client

    if _login_attempted:
        return client

    async with _login_lock:
        if _login_attempted:
            return client

        _login_attempted = True

        try:
            login_page = await client.get(f"{KINOZAL_BASE}/")
            login_html = _decode_html_bytes(login_page.content)

            action, form_data, username_field, password_field = _parse_login_form(login_html)
            if not action or not username_field or not password_field:
                log.warning("Kinozal login form not found or incomplete")
                return client

            form_data[username_field] = username
            form_data[password_field] = password

            resp = await client.post(
                action,
                data=form_data,
                headers={"Referer": f"{KINOZAL_BASE}/"},
            )
            _ = _decode_html_bytes(resp.content)

            verify = await client.get(f"{KINOZAL_BASE}/browse.php?s=&page=0&c=0&d=0&v=0")
            verify_html = _decode_html_bytes(verify.content)

            guest_markers = [
                'action="/takelogin.php"',
                "Гость! ( Зарегистрируйтесь )",
                "Регистрация",
            ]
            _login_ok = not any(marker in verify_html for marker in guest_markers)

            if _login_ok:
                log.info("Kinozal login successful")
            else:
                log.warning("Kinozal login failed: page still looks like guest view")

        except Exception:
            log.warning("Kinozal login failed with exception", exc_info=True)

    return client


async def fetch_kinozal_html(url: str) -> str:
    client = await _ensure_login()
    resp = await client.get(url)
    resp.raise_for_status()
    return _decode_html_bytes(resp.content)


async def fetch_kinozal_bytes(url: str) -> bytes:
    client = await _ensure_login()
    resp = await client.get(url)
    resp.raise_for_status()
    return resp.content


async def close_kinozal_http() -> None:
    global _client, _login_attempted, _login_ok
    if _client is not None:
        await _client.aclose()
    _client = None
    _login_attempted = False
    _login_ok = False
