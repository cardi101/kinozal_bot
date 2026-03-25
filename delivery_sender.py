import asyncio
import html as html_lib
import logging
import re
from typing import Any, Dict, List, Optional, Sequence

import httpx
from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.types import BufferedInputFile, InlineKeyboardMarkup

from config import CFG
from delivery_formatting import grouped_items_message, item_message
from keyboards import mute_title_kb
from kinozal_details import enrich_kinozal_item_with_details
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
            if not content or len(content) < 1024:
                log.info(
                    "Skip too-small poster for item=%s url=%s size=%s",
                    item_id,
                    poster_url,
                    len(content or b""),
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
    out: List[str] = []
    open_tags: List[str] = []

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
                        raw_close = raw_name.lower()
                        if raw_close in open_tags:
                            while open_tags:
                                last = open_tags.pop()
                                out.append(f"</{last}>")
                                if last == raw_close:
                                    break
                    else:
                        if tag_name == "a":
                            href_match = re.search(r'(?is)\bhref\s*=\s*(".*?"|\'.*?\'|[^\s>]+)', attrs)
                            if not href_match:
                                i = m.end()
                                continue
                        out.append(token)
                        open_tags.append(raw_name.lower())

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

    while open_tags:
        truncated += f"</{open_tags.pop()}>"

    return truncated


def _prepare_primary_item(item: Dict[str, Any]) -> Dict[str, Any]:
    prepared = dict(item)
    prepared["tmdb_overview"] = ""
    prepared["source_description"] = ""
    return prepared


def _normalize_release_text(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    lines: List[str] = []
    for raw_line in raw.splitlines():
        line = " ".join(str(raw_line or "").split()).strip()
        if not line:
            continue
        lines.append(line)

    return "\n".join(lines).strip()


def _build_release_followup_messages(item: Dict[str, Any], old_release_text: str = "", limit: int = 3500) -> List[str]:
    release_text = _normalize_release_text(item.get("source_release_text") or "")
    if not release_text:
        return []

    release_title = str(item.get("source_title") or item.get("tmdb_title") or "Без названия").strip()
    escaped_title = html_lib.escape(release_title)

    old_lines_set: set = set()
    removed_lines: List[str] = []
    if old_release_text:
        old_normalized = _normalize_release_text(old_release_text)
        old_lines_set = {l.strip() for l in old_normalized.splitlines() if l.strip()}
        new_lines_set = {l.strip() for l in release_text.splitlines() if l.strip()}
        removed_lines = [
            f"  ➖ {html_lib.escape(l)}"
            for l in old_normalized.splitlines()
            if l.strip() and l.strip() not in new_lines_set
        ]

    source_lines: List[str] = []
    for raw_line in release_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        escaped = html_lib.escape(line)
        if old_release_text and line not in old_lines_set:
            source_lines.append(f"  ➕ <b>{escaped}</b>")
        else:
            source_lines.append(f"  {escaped}")

    if not source_lines:
        return []

    if removed_lines:
        source_lines.extend([""] + removed_lines)

    messages: List[str] = []
    current_lines: List[str] = []

    def header(first: bool) -> str:
        if first:
            label = "🔄 <b>Релиз (изменился):</b>" if old_release_text else "📎 <b>Релиз:</b>"
            return f"{label} {escaped_title}"
        return f"📎 <b>Релиз (продолжение):</b> {escaped_title}"

    current_header = header(True)

    for line in source_lines:
        candidate_body = "\n".join(current_lines + [line]).strip()
        candidate_text = f"{current_header}\n\n{candidate_body}".strip()

        if current_lines and len(candidate_text) > limit:
            messages.append(f"{current_header}\n\n" + "\n".join(current_lines))
            current_header = header(False)
            current_lines = [line]
        else:
            current_lines.append(line)

    if current_lines:
        messages.append(f"{current_header}\n\n" + "\n".join(current_lines))

    return messages


async def _send_release_followups(bot: Bot, tg_user_id: int, item: Dict[str, Any], old_release_text: str = "") -> None:
    for text in _build_release_followup_messages(item, old_release_text=old_release_text):
        await bot.send_message(
            tg_user_id,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )


async def send_item_to_user(
    db: Any,
    bot: Bot,
    tg_user_id: int,
    item: Dict[str, Any],
    subs: Optional[Sequence[Dict[str, Any]]],
    old_release_text: str = "",
) -> None:
    primary_item = _prepare_primary_item(item)

    for key in ("source_link", "source_info_hash", "source_magnet", "imdb_id", "tmdb_id", "media_type"):
        if item.get(key) and not primary_item.get(key):
            primary_item[key] = item.get(key)

    try:
        primary_item = await enrich_kinozal_item_with_details(dict(primary_item))
    except Exception:
        log.warning(
            "Failed to enrich primary_item before formatting item=%s",
            primary_item.get("id"),
            exc_info=True,
        )

    text = item_message(db, primary_item, subs)
    text = _inject_compact_magnet_html(text, primary_item)

    tmdb_id = item.get("tmdb_id") or primary_item.get("tmdb_id")
    action_kb: Optional[InlineKeyboardMarkup] = mute_title_kb(int(tmdb_id)) if tmdb_id else None

    poster_url = item.get("tmdb_poster_url")
    full_html_text = short(text, 3900)
    full_plain_text = short(html_to_plain_text(text), 3900)

    main_sent = False

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
                    reply_markup=action_kb,
                )
                main_sent = True
            except Exception:
                log.warning(
                    "send_photo(uploaded poster) failed for user=%s item=%s",
                    tg_user_id,
                    item.get("id"),
                    exc_info=True,
                )

    if not main_sent:
        try:
            await bot.send_message(
                tg_user_id,
                text=full_html_text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=CFG.disable_preview,
                reply_markup=action_kb,
            )
            main_sent = True
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
                reply_markup=action_kb,
            )
            main_sent = True

    if main_sent:
        try:
            await _send_release_followups(bot, tg_user_id, item, old_release_text=old_release_text)
        except Exception:
            log.warning(
                "send release followup failed for user=%s item=%s",
                tg_user_id,
                item.get("id"),
                exc_info=True,
            )


async def send_grouped_items_to_user(
    db: Any,
    bot: Bot,
    tg_user_id: int,
    items: List[Dict[str, Any]],
    subs: Optional[Sequence[Dict[str, Any]]],
) -> None:
    text = grouped_items_message(db, items, subs)
    first = items[0]
    tmdb_id = first.get("tmdb_id")
    action_kb: Optional[InlineKeyboardMarkup] = mute_title_kb(int(tmdb_id)) if tmdb_id else None

    poster_url = first.get("tmdb_poster_url")
    sent = False
    if poster_url:
        poster_file = await _build_poster_file(str(poster_url), first.get("id"))
        if poster_file:
            caption_html = _safe_truncate_html(text, 1000)
            try:
                await bot.send_photo(
                    tg_user_id,
                    photo=poster_file,
                    caption=caption_html,
                    parse_mode=ParseMode.HTML,
                    reply_markup=action_kb,
                )
                sent = True
            except Exception:
                log.warning("send_photo failed for grouped delivery user=%s", tg_user_id, exc_info=True)
    if not sent:
        try:
            await bot.send_message(
                tg_user_id,
                text=short(text, 3900),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=CFG.disable_preview,
                reply_markup=action_kb,
            )
        except Exception:
            log.warning("send_message failed for grouped delivery user=%s", tg_user_id, exc_info=True)

    if sent:
        for item in items:
            if item.get("source_release_text"):
                try:
                    await _send_release_followups(bot, tg_user_id, item, old_release_text="")
                    await asyncio.sleep(0.12)
                except Exception:
                    log.warning("send_release_followup failed for grouped item user=%s", tg_user_id, exc_info=True)
