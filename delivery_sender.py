import html as html_lib
import logging
import re
from typing import Any, Dict, Optional, Sequence

import httpx
from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.types import BufferedInputFile

from config import CFG
from delivery_formatting import item_message
from magnet_links import build_public_magnet_redirect_url
from text_access import html_to_plain_text
from utils import short

log = logging.getLogger("kinozal-news-bot")

_ALLOWED_TAGS = {"b", "strong", "i", "em", "u", "ins", "s", "strike", "del", "code", "pre", "a"}
_VOID_TAGS = {"br"}


async def _build_poster_file(poster_url: str, item_id: Any) -> Optional[BufferedInputFile]:
    try:
        timeout = httpx.Timeout(CFG.request_timeout)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(
                poster_url,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            resp.raise_for_status()
            content_type = (resp.headers.get("content-type") or "").lower()
            if not content_type.startswith("image/"):
                log.info(
                    "Skip poster with non-image content-type for item=%s url=%s content-type=%s",
                    item_id,
                    poster_url,
                    content_type,
                )
                return None
            content = resp.content
            if not content:
                log.info(
                    "Skip empty poster body for item=%s url=%s",
                    item_id,
                    poster_url,
                )
                return None
            ext = "jpg"
            if "png" in content_type:
                ext = "png"
            elif "webp" in content_type:
                ext = "webp"
            elif "jpeg" in content_type or "jpg" in content_type:
                ext = "jpg"
            filename = f"poster_{item_id}.{ext}"
            return BufferedInputFile(content, filename=filename)
    except Exception:
        log.warning(
            "Failed to fetch poster for item=%s url=%s",
            item_id,
            poster_url,
            exc_info=True,
        )
        return None


def _inject_compact_magnet_html(text: str, item: Dict[str, Any]) -> str:
    public_url = build_public_magnet_redirect_url(item)
    if not public_url:
        return text

    magnet_html = f'<a href="{html_lib.escape(public_url, quote=True)}">🧲 Magnet</a>'

    if "\n🔗 " in text:
        lines = text.splitlines()
        for idx, line in enumerate(lines):
            if line.startswith("🔗 "):
                if "🧲 Magnet" not in line:
                    lines[idx] = f"{line} • {magnet_html}"
                return "\n".join(lines)

    return text.rstrip() + f"\n🔗 {magnet_html}"


def _html_entity_length(text: str, start: int) -> int:
    if text[start] != "&":
        return 1
    end = text.find(";", start + 1, min(len(text), start + 15))
    if end == -1:
        return 1
    return end - start + 1


def _normalize_tag_name(tag_name: str) -> str:
    tag_name = (tag_name or "").strip().lower()
    aliases = {
        "strong": "b",
        "em": "i",
        "ins": "u",
        "strike": "s",
        "del": "s",
    }
    return aliases.get(tag_name, tag_name)


def _safe_truncate_html(text: str, limit: int) -> str:
    if len(html_to_plain_text(text)) <= limit:
        return text

    i = 0
    visible = 0
    out: list[str] = []
    open_tags: list[str] = []

    tag_re = re.compile(r"(?is)<(/?)([a-zA-Z0-9]+)([^>]*)>")

    while i < len(text):
        if visible >= limit:
            break

        if text[i] == "<":
            m = tag_re.match(text, i)
            if m:
                closing = bool(m.group(1))
                raw_name = m.group(2)
                attrs = m.group(3) or ""
                tag_name = _normalize_tag_name(raw_name)

                if tag_name in _ALLOWED_TAGS or tag_name in _VOID_TAGS:
                    token = m.group(0)
                    if tag_name in _VOID_TAGS:
                        out.append(token)
                    elif closing:
                        if tag_name in open_tags:
                            while open_tags:
                                last = open_tags.pop()
                                out.append(f"</{last}>")
                                if last == tag_name:
                                    break
                    else:
                        if tag_name == "a":
                            href_match = re.search(r'(?is)\bhref\s*=\s*(".*?"|\'.*?\'|[^\s>]+)', attrs)
                            if not href_match:
                                i = m.end()
                                continue
                        out.append(token)
                        open_tags.append(tag_name)

                i = m.end()
                continue

        if text[i] == "&":
            entity_len = _html_entity_length(text, i)
            if visible + 1 > limit:
                break
            out.append(text[i:i + entity_len])
            visible += 1
            i += entity_len
            continue

        out.append(text[i])
        visible += 1
        i += 1

    truncated = "".join(out).rstrip()

    while truncated.endswith("<"):
        truncated = truncated[:-1].rstrip()

    if not truncated.endswith("…"):
        truncated += "…"

    while open_tags:
        truncated += f"</{open_tags.pop()}>"

    return truncated


async def send_item_to_user(
    db: Any,
    bot: Bot,
    tg_user_id: int,
    item: Dict[str, Any],
    subs: Optional[Sequence[Dict[str, Any]]],
) -> None:
    text = item_message(db, item, subs)
    text = _inject_compact_magnet_html(text, item)

    poster_url = item.get("tmdb_poster_url")
    full_html_text = short(text, 3900)
    full_plain_text = short(html_to_plain_text(text), 3900)

    if poster_url:
        poster_file = await _build_poster_file(str(poster_url), item.get("id"))
        if poster_file:
            caption_html = _safe_truncate_html(text, 1000)
            try:
                await bot.send_photo(
                    tg_user_id,
                    photo=poster_file,
                    caption=caption_html,
                    parse_mode=ParseMode.HTML,
                )
                return
            except Exception:
                log.warning(
                    "send_photo(uploaded poster) failed for user=%s item=%s",
                    tg_user_id,
                    item.get("id"),
                    exc_info=True,
                )

    try:
        await bot.send_message(
            tg_user_id,
            text=full_html_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=CFG.disable_preview,
        )
    except Exception:
        log.warning(
            "send_message HTML failed for user=%s item=%s",
            tg_user_id,
            item.get("id"),
            exc_info=True,
        )
        await bot.send_message(
            tg_user_id,
            text=full_plain_text,
            disable_web_page_preview=CFG.disable_preview,
        )
