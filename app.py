import asyncio
import hashlib
import html
import json
import logging
import os
import random
import re
import string
import threading
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from psycopg import connect
from psycopg.rows import dict_row
from urllib.parse import urlencode
from config import CFG, ACCESS_EXPIRY_UNSET
from states import EditInputState
from utils import utc_ts, now_utc, parse_dt, compact_spaces, strip_html, short, md5_text, sha1_text
from parsing_basic import parse_year, parse_years, parse_format, parse_imdb_id
from text_access import format_dt, user_access_state, format_access_expiry, human_media_type, html_to_plain_text, require_access_message
from source_categories import normalize_source_category_id, resolve_source_category_name, source_category_is_non_video, source_category_forced_media_type, source_category_bucket_hint, source_category_fallback_country_codes
from release_versioning import parse_episode_progress, normalize_episode_progress_signature, extract_kinozal_id, resolve_item_kinozal_id, build_source_uid, normalize_audio_tracks_signature, version_release_type_signature, build_variant_signature, build_item_variant_signature, get_variant_components, get_item_variant_components, describe_variant_change, format_variant_summary, build_version_signature
from country_helpers import COUNTRY_NAMES_RU, ANIME_COUNTRY_CODES, parse_jsonish_list, parse_country_codes, country_name_ru, human_country_names, effective_item_countries, normalize_tmdb_language, has_asian_script, asian_dorama_signal_score, human_content_filter
from item_years import item_source_years, min_year_delta, extract_expected_tv_totals, extract_tv_season_hint, item_filter_years, item_display_year
from media_detection import is_non_video_release, detect_media_type
from keyword_filters import parse_rating, normalize_keywords_input, build_keyword_haystacks, keyword_matches_item
from title_prep import clean_release_title, looks_like_structured_numeric_title, is_release_group_candidate, normalize_structured_numeric_title, extract_structured_numeric_title_candidates, should_skip_tmdb_lookup, extract_title_aliases_from_text, split_title_parts, is_bad_tmdb_candidate
from match_text import similarity, is_generic_cyrillic_title, normalize_match_text, text_tokens, raw_text_tokens, token_overlap_ratio
from tmdb_aliases import ANIME_TITLE_MARKER_RE, expand_tmdb_candidate_variants, is_long_latin_tmdb_query, is_short_or_common_tmdb_query, is_short_acronym_tmdb_query, manual_tmdb_override_for_item, manual_alias_candidates_from_text, anime_alias_candidates_from_text, title_search_candidates
from content_buckets import anime_fallback_signal_score, item_content_bucket
from tmdb_match_validation import is_anime_franchise_parent_fallback, is_tv_continuation_parent_match, is_tv_revival_reset_match, tmdb_match_looks_valid
from subscription_presets import PRESET_ROLLOUT_VERSION, subscription_presets, apply_subscription_preset, detect_subscription_preset_key
from genres_helpers import item_genre_names, sub_genre_names
from subscription_matching import match_subscription
from subscription_text import sub_summary
from delivery_formatting import item_message
from service_helpers import safe_edit, _exc_brief, send_admins_text
from source_health import _meta_int, note_source_cycle_success, note_source_cycle_failure
from delivery_sender import send_item_to_user
from admin_helpers import is_admin, extract_kinozal_id_from_text, parse_admin_route_target, format_admin_user_line, format_admin_user_details, parse_command_payload
from access_helpers import ensure_access_for_message, ensure_access_for_callback
from dynamic_keyboards import genres_kb, countries_kb, content_filter_kb
from match_debug_helpers import build_match_explanation, rematch_item_live
from subscription_test_helpers import get_live_test_items_for_subscription
from menu_views import show_main_menu
from menu_handlers import register_menu_handlers
from subscription_basic_handlers import register_subscription_basic_handlers
from subscription_filter_handlers import register_subscription_filter_handlers
from subscription_input_handlers import register_subscription_input_handlers
from subscription_wizard_handlers import register_subscription_wizard_handlers
from subscription_test_handlers import register_subscription_test_handlers
from user_handlers import register_user_handlers
from admin_match_handlers import register_admin_match_handlers
from admin_access_handlers import register_admin_access_handlers
from runtime_poller import process_new_items, poller
from runtime_app import AppRuntime
from redis_cache import RedisCache
from tmdb_client import TMDBClient
from kinozal_source import KinozalSource
from db import DB
from parsing_audio import parse_audio_variants, format_audio_variants, count_audio_variants, parse_audio_tracks, infer_release_type, format_release_full_title
from keyboards import main_menu_kb, subscriptions_list_kb, sub_view_kb, sub_type_kb, year_preset_kb, rating_kb, format_kb, preset_kb, wizard_type_kb, wizard_years_kb, wizard_rating_kb, admin_invites_kb, admin_users_kb

try:
    import pycountry
except Exception:
    pycountry = None

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("kinozal-news-bot")


db = DB(CFG.database_url)


cache = RedisCache(CFG.redis_url)


tmdb = TMDBClient(CFG, db, cache, CFG.tmdb_token, CFG.language, log)


source = KinozalSource()


router = Router()
ADMIN_USERS_PAGE_SIZE = 12


runtime = AppRuntime(
    CFG,
    router,
    db,
    source,
    tmdb,
    cache,
    poller,
    log,
    PRESET_ROLLOUT_VERSION,
)


register_menu_handlers(router, db, source, tmdb, ADMIN_USERS_PAGE_SIZE)
register_subscription_basic_handlers(router, db)
register_subscription_filter_handlers(router, db)
register_subscription_input_handlers(router, db)
register_subscription_wizard_handlers(router, db)
register_subscription_test_handlers(router, db, source, tmdb)
register_user_handlers(router, db, source, tmdb)
register_admin_match_handlers(router, db, tmdb)
register_admin_access_handlers(router, db, ADMIN_USERS_PAGE_SIZE)


@router.callback_query(F.data == "noop")
async def cb_noop(callback: CallbackQuery) -> None:
    await callback.answer()


if __name__ == "__main__":
    asyncio.run(runtime.main())
