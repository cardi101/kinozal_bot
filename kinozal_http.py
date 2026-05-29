import asyncio
import logging
import os
import re
from html import unescape
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import httpx
from ftfy import fix_text

log = logging.getLogger("kinozal-http")

KINOZAL_BASE = "https://kinozal.tv"
KINOZAL_DEFAULT_MIRRORS = "https://kinozal.guru"

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

_client: Optional[httpx.AsyncClient] = None
_client_lock = asyncio.Lock()
_login_lock = asyncio.Lock()
_login_attempted_by_base: Dict[str, bool] = {}
_login_ok_by_base: Dict[str, bool] = {}


def _normalize_base_url(value: str) -> str:
    raw = str(value or "").strip().rstrip("/")
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    if not parsed.netloc:
        return ""
    scheme = parsed.scheme or "https"
    return urlunparse((scheme, parsed.netloc, "", "", "", ""))


def _split_base_urls(value: str) -> List[str]:
    urls: List[str] = []
    for part in re.split(r"[\s,]+", str(value or "")):
        base = _normalize_base_url(part)
        if base and base not in urls:
            urls.append(base)
    return urls


def get_kinozal_base_urls() -> List[str]:
    primary = _normalize_base_url(os.getenv("KINOZAL_BASE_URL", KINOZAL_BASE)) or KINOZAL_BASE
    mirrors = _split_base_urls(
        os.getenv("KINOZAL_MIRROR_BASE_URLS", KINOZAL_DEFAULT_MIRRORS)
    )

    urls: List[str] = []
    for base in [primary, *mirrors]:
        if base and base not in urls:
            urls.append(base)
    return urls or [KINOZAL_BASE]


def kinozal_url(path: str, base_url: str = "") -> str:
    base = _normalize_base_url(base_url) or get_kinozal_base_urls()[0]
    return urljoin(f"{base}/", str(path or "").lstrip("/"))


def get_kinozal_browse_urls() -> List[str]:
    return [
        kinozal_url("/browse.php?s=&page=0&c=0&d=0&v=0", base)
        for base in get_kinozal_base_urls()
    ]


def get_kinozal_url_base(url: str) -> str:
    parsed = urlparse(str(url or ""))
    if not parsed.netloc:
        return get_kinozal_base_urls()[0]
    return (
        _normalize_base_url(f"{parsed.scheme or 'https'}://{parsed.netloc}")
        or get_kinozal_base_urls()[0]
    )


def _url_candidates(url: str) -> List[str]:
    raw = str(url or "").strip()
    if not raw:
        return []

    parsed = urlparse(raw)
    if not parsed.netloc:
        path = raw
        return [kinozal_url(path, base) for base in get_kinozal_base_urls()]

    original_base = get_kinozal_url_base(raw)
    known_bases = get_kinozal_base_urls()
    known_hosts = {urlparse(base).netloc for base in known_bases}
    if urlparse(original_base).netloc not in known_hosts:
        return [raw]

    path = urlunparse(("", "", parsed.path or "/", parsed.params, parsed.query, parsed.fragment))
    bases = [original_base, *[base for base in known_bases if base != original_base]]
    urls: List[str] = []
    for base in bases:
        candidate = kinozal_url(path, base)
        if candidate not in urls:
            urls.append(candidate)
    return urls


def _request_timeout() -> float:
    raw = os.getenv("REQUEST_TIMEOUT", "").strip()
    if not raw:
        return 60.0
    try:
        return float(raw)
    except Exception:
        return 60.0


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


def _parse_login_form(html: str, base_url: str = "") -> Tuple[str, Dict[str, str], str, str]:
    form_match = re.search(
        r'(?is)<form[^>]+action=["\']([^"\']*takelogin\.php[^"\']*)["\'][^>]*>(.*?)</form>',
        html,
    )
    if not form_match:
        return "", {}, "", ""

    action_base = _normalize_base_url(base_url) or get_kinozal_base_urls()[0]
    action = urljoin(f"{action_base}/", unescape(form_match.group(1)))
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


def _guest_markers() -> Tuple[str, ...]:
    return (
        'action="/takelogin.php"',
        "Гость! ( Зарегистрируйтесь )",
        "Регистрация",
        "Вы не зарегистрированный пользователь",
        "не авторизированы",
    )


def _looks_like_guest_page(text: str) -> bool:
    sample = str(text or "")
    return any(marker in sample for marker in _guest_markers())


async def _get_client() -> httpx.AsyncClient:
    global _client

    async with _client_lock:
        if _client is None or getattr(_client, "is_closed", False):
            _client = httpx.AsyncClient(
                timeout=httpx.Timeout(_request_timeout()),
                follow_redirects=True,
                headers={
                    "User-Agent": _USER_AGENT,
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "ru,en;q=0.9",
                    "Referer": f"{get_kinozal_base_urls()[0]}/",
                },
            )

        return _client


async def _ensure_login(base_url: str = "", force: bool = False) -> httpx.AsyncClient:
    client = await _get_client()
    base_url = _normalize_base_url(base_url) or get_kinozal_base_urls()[0]
    username = os.getenv("KINOZAL_USERNAME", "").strip()
    password = os.getenv("KINOZAL_PASSWORD", "").strip()

    if not username or not password:
        return client

    async with _login_lock:
        if force:
            _login_attempted_by_base[base_url] = False
            _login_ok_by_base[base_url] = False

        if _login_attempted_by_base.get(base_url) and _login_ok_by_base.get(base_url):
            return client

        _login_attempted_by_base[base_url] = True

        try:
            login_page = await client.get(f"{base_url}/", headers={"Referer": f"{base_url}/"})
            login_page.raise_for_status()
            login_html = _decode_html_bytes(login_page.content)

            action, form_data, username_field, password_field = _parse_login_form(login_html, base_url)
            if not action or not username_field or not password_field:
                log.warning("Kinozal login form not found or incomplete base=%s", base_url)
                return client

            form_data[username_field] = username
            form_data[password_field] = password

            resp = await client.post(
                action,
                data=form_data,
                headers={"Referer": f"{base_url}/"},
            )
            resp.raise_for_status()
            _ = _decode_html_bytes(resp.content)

            verify = await client.get(kinozal_url("/browse.php?s=&page=0&c=0&d=0&v=0", base_url))
            verify.raise_for_status()
            verify_html = _decode_html_bytes(verify.content)

            _login_ok_by_base[base_url] = not _looks_like_guest_page(verify_html)

            if _login_ok_by_base.get(base_url):
                log.info("Kinozal login successful base=%s", base_url)
            else:
                log.warning("Kinozal login failed: page still looks like guest view base=%s", base_url)

        except Exception:
            log.warning("Kinozal login failed with exception base=%s", base_url, exc_info=True)

    return client


def _score_decoded_text(text: str) -> int:
    text = str(text or "")
    low = text.lower()
    score = 0

    good_markers = [
        "аудио",
        "субтитры",
        "перевод",
        "перевод и озвучивание",
        "озвучивание",
        "озвучка",
        "релиз",
        "автор релиза",
        "без рекламы",
        "реклама",
        "звук",
        "качество",
        "видео",
        "размер",
        "продолжительность",
        "русский",
        "русские",
        "английский",
        "английские",
        "японский",
        "японские",
        "комментарии",
        "раздают",
        "скачивают",
        "список файлов",
        "раздачи трекера",
        "меню раздачи",
        "подобные раздачи",
    ]
    for marker in good_markers:
        if marker in low:
            score += 20

    score += len(re.findall(r"[А-Яа-яЁё]", text))

    score -= len(re.findall(r"[РС][Ѓ-џ]", text)) * 12
    score -= text.count("Р›С") * 10
    score -= text.count("РЎС") * 10
    score -= text.count("РђС") * 10
    score -= text.count("Р°Р") * 8
    score -= text.count("СЃ") * 4
    score -= text.count("Р ")
    score -= text.count("С ")

    return score


def _decode_kinozal_bytes(raw: bytes, content_type: str = "") -> str:
    if not raw:
        return ""

    candidates = []

    def add(value: str) -> None:
        value = str(value or "").replace("\x00", "")
        if value and value not in candidates:
            candidates.append(value)

    m = re.search(r"charset=([a-zA-Z0-9_\-]+)", content_type or "", flags=re.I)
    if m:
        enc = m.group(1).strip().lower()
        try:
            add(raw.decode(enc))
        except Exception:
            pass

    for enc in ("utf-8", "cp1251", "windows-1251", "latin1"):
        try:
            add(raw.decode(enc))
        except Exception:
            pass

    base = list(candidates)
    for item in base:
        try:
            add(fix_text(item))
        except Exception:
            pass

    second = list(candidates)
    for item in second:
        try:
            add(fix_text(fix_text(item)))
        except Exception:
            pass

    if not candidates:
        return raw.decode("utf-8", errors="replace").replace("\x00", "")

    return max(candidates, key=_score_decoded_text)


async def get_kinozal_http_client(base_url: str = "") -> httpx.AsyncClient:
    return await _ensure_login(base_url)


async def _fetch_single_kinozal_response(url: str) -> httpx.Response:
    base_url = get_kinozal_url_base(url)
    client = await get_kinozal_http_client(base_url)

    response = await client.get(url, headers={"Referer": f"{base_url}/"})
    response.raise_for_status()

    username = os.getenv("KINOZAL_USERNAME", "").strip()
    password = os.getenv("KINOZAL_PASSWORD", "").strip()
    have_creds = bool(username and password)

    if have_creds:
        raw = response.content or b""
        decoded = _decode_kinozal_bytes(raw, response.headers.get("content-type", ""))
        if _looks_like_guest_page(decoded):
            log.warning("Kinozal request looks unauthorized, retrying after forced login: %s", url)
            client = await _ensure_login(base_url, force=True)
            response = await client.get(url, headers={"Referer": f"{base_url}/"})
            response.raise_for_status()
            raw2 = response.content or b""
            decoded2 = _decode_kinozal_bytes(raw2, response.headers.get("content-type", ""))
            if _looks_like_guest_page(decoded2):
                raise RuntimeError(
                    "Kinozal login failed: still getting guest page after forced re-login"
                )

    return response


async def _fetch_kinozal_response(url: str) -> httpx.Response:
    candidates = _url_candidates(url)
    if not candidates:
        raise ValueError("Kinozal URL is empty")

    last_exc: Exception | None = None
    for idx, candidate in enumerate(candidates):
        try:
            response = await _fetch_single_kinozal_response(candidate)
            if candidate != candidates[0]:
                log.info(
                    "Kinozal mirror request succeeded original=%s mirror=%s",
                    candidates[0],
                    candidate,
                )
            return response
        except Exception as exc:
            last_exc = exc
            if idx + 1 < len(candidates):
                log.warning(
                    "Kinozal request failed, trying mirror url=%s next=%s",
                    candidate,
                    candidates[idx + 1],
                    exc_info=True,
                )

    assert last_exc is not None
    raise last_exc


async def fetch_kinozal_bytes(url: str) -> bytes:
    response = await _fetch_kinozal_response(url)
    return response.content or b""


async def fetch_kinozal_html(url: str) -> str:
    response = await _fetch_kinozal_response(url)
    raw = response.content or b""
    return _decode_kinozal_bytes(raw, response.headers.get("content-type", ""))


async def fetch_kinozal_html_with_url(url: str) -> Tuple[str, str]:
    response = await _fetch_kinozal_response(url)
    raw = response.content or b""
    html = _decode_kinozal_bytes(raw, response.headers.get("content-type", ""))
    return str(response.url), html


async def close_kinozal_http() -> None:
    global _client

    async with _login_lock:
        _login_attempted_by_base.clear()
        _login_ok_by_base.clear()

    async with _client_lock:
        if _client is not None and not getattr(_client, "is_closed", False):
            await _client.aclose()
        _client = None
