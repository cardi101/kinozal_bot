import asyncio
import json
import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx

from country_helpers import normalize_tmdb_language
from match_text import similarity, normalize_match_text, text_tokens
from media_detection import is_non_video_release
from source_categories import source_category_forced_media_type
from parsing_basic import parse_year
from title_prep import clean_release_title, classify_release_segments, looks_like_structured_numeric_title, normalize_structured_numeric_title, should_skip_tmdb_lookup, is_bad_tmdb_candidate
from tmdb_aliases import is_long_latin_tmdb_query, is_short_or_common_tmdb_query, is_short_acronym_tmdb_query, manual_tmdb_override_for_item, title_search_candidates
from item_years import extract_tv_season_hint
from tmdb_match_features import extract_tmdb_match_features, score_tmdb_match_candidate
from tmdb_match_validation import tmdb_match_looks_valid, tmdb_validation_reason_code
from anime_mapping_store import AnimeMappingStore
from anime_title_lexicon import AnimeTitleLexicon
from anime_resolver import resolve_anime_tmdb, should_use_anime_resolver
from utils import utc_ts, compact_spaces
from release_versioning import resolve_item_kinozal_id


_GENERIC_SINGLE_TOKEN_ANIME_ALIASES = {
    "fate",
    "destiny",
    "unmei",
    "promise",
    "legend",
    "hero",
    "world",
    "dream",
    "story",
    "love",
}


def _normalize_anime_guard_text(value: Any) -> str:
    return normalize_match_text(compact_spaces(str(value or "")))


def _extract_slash_title_candidates(raw_title: str) -> List[str]:
    raw_title = compact_spaces(raw_title or "")
    if not raw_title:
        return []

    out: List[str] = []
    title_segments = [
        compact_spaces(str(segment.get("title") or segment.get("raw") or ""))
        for segment in classify_release_segments(raw_title)
        if str(segment.get("kind") or "").startswith("title")
    ]
    combined = compact_spaces(" / ".join(segment for segment in title_segments if segment))
    if combined and "/" in combined:
        out.append(combined)
        combined_spaced = compact_spaces(combined.replace("/", " "))
        if combined_spaced and combined_spaced not in out:
            out.append(combined_spaced)

    return out


def _should_skip_generic_lexicon_expansion(item: Dict[str, Any], lexicon_best: Any) -> bool:
    source_title = compact_spaces(item.get("source_title") or "")
    cleaned_title = compact_spaces(item.get("cleaned_title") or "")

    source_norm = _normalize_anime_guard_text(source_title)
    cleaned_norm = _normalize_anime_guard_text(cleaned_title)
    combined_norm = " ".join(x for x in [source_norm, cleaned_norm] if x).strip()

    if not combined_norm:
        return False

    source_looks_specific = ("/" in source_title) or (len(combined_norm.split()) >= 2)
    if not source_looks_specific:
        return False

    matched_generic = False
    matched_specific = False

    for alias in getattr(lexicon_best, "titles", [])[:20]:
        alias_norm = _normalize_anime_guard_text(alias)
        if not alias_norm:
            continue
        if alias_norm not in combined_norm:
            continue

        if len(alias_norm.split()) == 1 and alias_norm in _GENERIC_SINGLE_TOKEN_ANIME_ALIASES:
            matched_generic = True
        elif len(alias_norm.split()) >= 2 or len(alias_norm) >= 12:
            matched_specific = True

    canonical_norm = _normalize_anime_guard_text(getattr(lexicon_best, "canonical_title", ""))
    if canonical_norm and canonical_norm in combined_norm:
        if len(canonical_norm.split()) >= 2 or len(canonical_norm) >= 12:
            matched_specific = True

    return matched_generic and not matched_specific


def _contains_cjk_or_kana(value: str) -> bool:
    return bool(re.search(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]", value or ""))


def _should_expand_lexicon_alias(
    item: Dict[str, Any],
    fallback_cleaned_title: str,
    lexicon_best: Any,
    value: str,
) -> bool:
    value = compact_spaces(value or "")
    if not value:
        return False

    if _contains_cjk_or_kana(value):
        return True

    value_norm = _normalize_anime_guard_text(value)
    if not value_norm:
        return False
    if len(value_norm.split()) == 1 and value_norm in _GENERIC_SINGLE_TOKEN_ANIME_ALIASES:
        return False

    candidate_tokens = set(text_tokens(value_norm))
    source_variants = [
        fallback_cleaned_title,
        item.get("cleaned_title") or "",
        item.get("source_title") or "",
        getattr(lexicon_best, "canonical_title", "") or "",
    ]
    for source_value in source_variants:
        source_clean = compact_spaces(clean_release_title(source_value or "") or source_value or "")
        source_norm = _normalize_anime_guard_text(source_clean)
        if not source_norm:
            continue
        if value_norm == source_norm or value_norm in source_norm or source_norm in value_norm:
            return True
        if candidate_tokens and candidate_tokens & set(text_tokens(source_norm)):
            return True
        if similarity(value_norm, source_norm) >= 0.72:
            return True
    return False


_GENERIC_CYRILLIC_SEARCH_TOKENS = {
    "дело",
    "фильм",
    "серия",
    "эпизод",
    "выпуск",
    "новости",
    "матч",
    "концерт",
    "шоу",
    "эфир",
}


def _should_skip_generic_search_candidate(item: Dict[str, Any], candidate: str) -> bool:
    candidate = compact_spaces(str(candidate or ""))
    if not candidate:
        return True

    if looks_like_structured_numeric_title(candidate):
        return False

    norm = normalize_match_text(candidate)
    norm = norm.replace("№", " ").replace("#", " ")
    tokens = [tok for tok in re.findall(r"[a-zа-я0-9]+", norm, flags=re.I) if len(tok) > 1]

    # Совсем мусорные/сверхобщие кейсы: "Дело", "Дело №", "Фильм", "Серия" и т.п.
    if len(tokens) == 0:
        return True

    if len(tokens) == 1:
        tok = tokens[0]
        if tok in _GENERIC_CYRILLIC_SEARCH_TOKENS:
            return True

    if ("№" in candidate or "#" in candidate) and len(tokens) <= 1:
        return True

    # Для кириллицы с одним слабым токеном лучше не матчить автоматически.
    if re.search(r"[А-Яа-яЁё]", candidate) and len(tokens) <= 1:
        return True

    return False


def _is_hard_blocked_generic_candidate(candidate: str) -> bool:
    candidate = compact_spaces(str(candidate or "")).lower()
    if not candidate:
        return True

    candidate = candidate.replace("№", " ").replace("#", " ")
    candidate = re.sub(r"\s+", " ", candidate).strip()

    hard_block_exact = {
        "дело",
        "фильм",
        "серия",
        "эпизод",
        "выпуск",
        "эфир",
        "матч",
        "новости",
        "концерт",
        "шоу",
    }

    if candidate in hard_block_exact:
        return True

    if re.fullmatch(r"(дело|фильм|серия|эпизод|выпуск)\s+\d+", candidate):
        return True

    return False



def _fallback_cleaned_title_from_source_title(source_title: str) -> str:
    source_title = compact_spaces(source_title or "")
    if not source_title:
        return ""

    parts = [compact_spaces(x) for x in source_title.split(" / ") if compact_spaces(x)]
    if not parts:
        return ""

    title_parts = []
    for part in parts:
        if re.fullmatch(r"(19|20)\d{2}(?:-(19|20)\d{2})?", part):
            break

        if re.search(
            r"\b(WEB|WEBRip|WEB-DL|BDRip|Blu-?Ray|DVDRip|HEVC|AVC|HDR|2160p|1080p|720p|x264|x265|RU|РУ|ЛМ|ПМ|СТ|ДБ|РМ)\b",
            part,
            flags=re.I,
        ):
            break

        cleaned_part = re.sub(r"\s*\([^)]*сезон[^)]*\)\s*$", "", part, flags=re.I)
        cleaned_part = re.sub(r"\s*\([^)]*серии?[^)]*\)\s*$", "", cleaned_part, flags=re.I)
        cleaned_part = compact_spaces(cleaned_part)

        if cleaned_part and cleaned_part not in title_parts:
            title_parts.append(cleaned_part)

    return " / ".join(title_parts[:2])


def _title_variants_for_confidence(item: Dict[str, Any], details: Dict[str, Any]) -> tuple[list[str], list[str]]:
    def _expand_variants(values: list[str]) -> list[str]:
        seen: set[str] = set()
        expanded: list[str] = []

        def _push(raw: str) -> None:
            value = compact_spaces(raw)
            if not value:
                return
            norm = normalize_match_text(value)
            if not norm or norm in seen:
                return
            seen.add(norm)
            expanded.append(value)

        for value in values:
            _push(value)
            for part in [compact_spaces(part) for part in value.split(" / ") if compact_spaces(part)]:
                _push(part)
                cleaned_part = re.sub(r"\s*\([^)]*сезон[^)]*\)\s*$", "", part, flags=re.I)
                cleaned_part = re.sub(r"\s*\([^)]*серии?[^)]*\)\s*$", "", cleaned_part, flags=re.I)
                _push(cleaned_part)
                for bracket in re.findall(r"\(([^)]+)\)", part):
                    if re.search(r"сезон|серии?", bracket, flags=re.I):
                        continue
                    _push(bracket)

        return expanded

    item_variants = _expand_variants(
        [
            compact_spaces(item.get("source_title") or ""),
            compact_spaces(item.get("cleaned_title") or ""),
        ]
    )
    detail_variants = _expand_variants(
        [
            compact_spaces(details.get("search_match_title") or ""),
            compact_spaces(details.get("search_match_original_title") or ""),
            compact_spaces(details.get("tmdb_title") or ""),
            compact_spaces(details.get("tmdb_original_title") or ""),
        ]
    )
    return item_variants, detail_variants


def _match_overlap(item: Dict[str, Any], details: Dict[str, Any]) -> tuple[float, float]:
    item_variants, detail_variants = _title_variants_for_confidence(item, details)
    best_similarity = 0.0
    best_overlap = 0.0
    for left in item_variants:
        left_tokens = set(text_tokens(left))
        left_norm = normalize_match_text(left)
        for right in detail_variants:
            right_tokens = set(text_tokens(right))
            right_norm = normalize_match_text(right)
            if left_norm and right_norm:
                best_similarity = max(best_similarity, similarity(left_norm, right_norm))
            if left_tokens and right_tokens:
                best_overlap = max(best_overlap, len(left_tokens & right_tokens) / max(len(left_tokens), len(right_tokens), 1))
    return best_similarity, best_overlap


def _search_match_confidence(item: Dict[str, Any], details: Dict[str, Any]) -> tuple[str, str]:
    best_similarity, best_overlap = _match_overlap(item, details)
    source_year = parse_year(str(item.get("source_year") or ""))
    tmdb_year = parse_year(str(details.get("tmdb_release_date") or ""))
    year_delta = abs(source_year - tmdb_year) if source_year and tmdb_year else None
    item_variants, detail_variants = _title_variants_for_confidence(item, details)
    normalized_item = {normalize_match_text(value) for value in item_variants if value}
    normalized_detail = {normalize_match_text(value) for value in detail_variants if value}
    exact = bool(normalized_item & normalized_detail)

    evidence_parts = [
        f"exact={int(exact)}",
        f"similarity={best_similarity:.3f}",
        f"overlap={best_overlap:.3f}",
    ]
    if year_delta is not None:
        evidence_parts.append(f"year_delta={year_delta}")

    if exact and (year_delta is None or year_delta <= 1):
        return "high", ", ".join(evidence_parts)
    if exact and (year_delta is None or year_delta <= 2):
        return "medium", ", ".join(evidence_parts)
    if best_similarity >= 0.93 and best_overlap >= 0.75 and (year_delta is None or year_delta <= 2):
        return "high", ", ".join(evidence_parts)
    if best_similarity >= 0.86 and best_overlap >= 0.52 and (year_delta is None or year_delta <= 2):
        return "medium", ", ".join(evidence_parts)
    if year_delta is not None and year_delta >= 4:
        return "low", ", ".join(evidence_parts)
    if best_similarity < 0.62:
        return "low", ", ".join(evidence_parts)
    if best_similarity < 0.76 and best_overlap < 0.34:
        return "low", ", ".join(evidence_parts)
    return "medium", ", ".join(evidence_parts)




class TMDBClient:
    def __init__(self, cfg: Any, db: Any, cache: Any, token: str, language: str, log: logging.Logger):
        self.anime_title_lexicon = None
        if cfg.anime_resolver_enabled or cfg.anime_resolver_log_only:
            try:
                self.anime_title_lexicon = AnimeTitleLexicon(cfg.anime_mappings_dir)
                self.anime_title_lexicon.load()
                logging.getLogger(__name__).info(
                    "Anime lexicon loaded dir=%s entries=%s",
                    cfg.anime_mappings_dir,
                    len(getattr(self.anime_title_lexicon, "entries", []) or []),
                )
            except Exception:
                logging.getLogger(__name__).exception(
                    "Failed to initialize anime title lexicon dir=%s",
                    cfg.anime_mappings_dir,
                )
                self.anime_title_lexicon = None

        self.anime_mapping_store = None
        if cfg.anime_resolver_enabled or cfg.anime_resolver_log_only:
            try:
                self.anime_mapping_store = AnimeMappingStore(cfg.anime_mappings_dir)
                self.anime_mapping_store.load()
                logging.getLogger(__name__).info(
                    "Anime resolver mapping store loaded dir=%s entries=%s",
                    cfg.anime_mappings_dir,
                    len(getattr(self.anime_mapping_store, 'entries', []) or []),
                )
            except Exception:
                logging.getLogger(__name__).exception(
                    "Failed to initialize anime resolver mapping store dir=%s",
                    cfg.anime_mappings_dir,
                )
                self.anime_mapping_store = None

        self.cfg = cfg
        self.db = db
        self.cache = cache
        self.log = log
        self.token = token
        self.language = language
        self.client = httpx.AsyncClient(timeout=self.cfg.request_timeout)
        self.base = "https://api.themoviedb.org/3"

    async def close(self) -> None:
        await self.client.aclose()

    def _kinozal_override(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if bool(item.get("_skip_kinozal_override")):
            return None
        return self.db.get_match_override(resolve_item_kinozal_id(item) or "")

    def _is_rejected_match(self, item: Dict[str, Any], details: Optional[Dict[str, Any]]) -> bool:
        if not details or not details.get("tmdb_id"):
            return False
        kinozal_id = resolve_item_kinozal_id(item) or ""
        if not kinozal_id:
            return False
        try:
            return self.db.is_match_rejected(kinozal_id, int(details["tmdb_id"]))
        except Exception:
            return False

    def _apply_match_metadata(
        self,
        item: Dict[str, Any],
        details: Dict[str, Any],
        path: str,
        confidence: str,
        evidence: str,
    ) -> None:
        item["tmdb_match_path"] = compact_spaces(path)
        item["tmdb_match_confidence"] = compact_spaces(confidence)
        item["tmdb_match_evidence"] = compact_spaces(evidence)

    def _record_match_debug(self, item: Dict[str, Any], stage: str, **payload: Any) -> None:
        events = item.setdefault("_tmdb_match_debug_events", [])
        if not isinstance(events, list):
            events = []
            item["_tmdb_match_debug_events"] = events
        event = {"stage": compact_spaces(stage)}
        for key, value in payload.items():
            if value is None:
                continue
            if isinstance(value, float):
                event[key] = round(value, 3)
            else:
                event[key] = value
        events.append(event)

    def _finalize_match_debug(self, item: Dict[str, Any]) -> None:
        events = item.pop("_tmdb_match_debug_events", None)
        if not events:
            item["tmdb_match_debug"] = ""
            return
        item["tmdb_match_debug"] = json.dumps(events[-40:], ensure_ascii=False)

    def _build_search_plan(
        self,
        item: Dict[str, Any],
        candidates: List[str],
        media_type: str,
        year: Optional[int],
    ) -> List[Tuple[str, str, Optional[int]]]:
        search_plan: List[Tuple[str, str, Optional[int]]] = []
        strict_tv_only = bool(item.get("source_episode_progress")) or media_type == "tv"
        looks_like_series = strict_tv_only or bool(extract_tv_season_hint(item))
        continuation_tv = looks_like_series and bool(extract_tv_season_hint(item) and extract_tv_season_hint(item) >= 2)

        for candidate in candidates:
            if media_type == "tv" or looks_like_series:
                tv_years: List[Optional[int]] = []
                if continuation_tv:
                    tv_years.extend([None, year, year - 1 if year else None, year + 1 if year else None])
                else:
                    tv_years.extend([year, None, year - 1 if year else None])
                seen_years: set[Optional[int]] = set()
                for tv_year in tv_years:
                    if tv_year in seen_years:
                        continue
                    seen_years.add(tv_year)
                    search_plan.append((candidate, "tv", tv_year))
                if not strict_tv_only:
                    search_plan.extend([(candidate, "movie", year), (candidate, "movie", None)])
            else:
                search_plan.extend([(candidate, "movie", year), (candidate, "movie", None), (candidate, "tv", None), (candidate, "tv", year)])
        return search_plan

    async def search_candidates_for_item(self, item: Dict[str, Any], limit: int = 5) -> List[Dict[str, Any]]:
        if not self.token:
            return []

        fallback_cleaned_title = compact_spaces(item.get("cleaned_title") or "")
        if not fallback_cleaned_title:
            fallback_cleaned_title = _fallback_cleaned_title_from_source_title(item.get("source_title") or "")

        media_type = compact_spaces(str(item.get("media_type") or "movie")).lower() or "movie"
        year = item.get("source_year")
        candidates = title_search_candidates(
            item.get("source_title") or "",
            fallback_cleaned_title or "",
        )
        extra_slash_candidates = _extract_slash_title_candidates(item.get("source_title") or "")
        if extra_slash_candidates:
            candidates = extra_slash_candidates + [value for value in candidates if value not in extra_slash_candidates]

        queries = [candidate for candidate in candidates if not _is_hard_blocked_generic_candidate(candidate)]
        queries = queries[:6]

        search_plan = self._build_search_plan(item, queries, media_type, year)

        results: List[Dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()
        for candidate, target_media_type, candidate_year in search_plan:
            try:
                ranked_details = await self.search_ranked(candidate, target_media_type, candidate_year, limit=max(2, limit))
            except Exception:
                continue

            for probe in ranked_details:
                tmdb_id = int(probe.get("tmdb_id") or 0)
                if tmdb_id <= 0:
                    continue
                key = (target_media_type, tmdb_id)
                if key in seen:
                    continue
                if self._is_rejected_match(item, probe):
                    continue
                features = extract_tmdb_match_features(item, candidate, probe, target_media_type)
                confidence, evidence = _search_match_confidence(item, probe)
                results.append(
                    {
                        "tmdb_id": tmdb_id,
                        "media_type": target_media_type,
                        "title": probe["tmdb_title"] or probe["tmdb_original_title"] or "—",
                        "original_title": probe["tmdb_original_title"] or "",
                        "release_date": probe["tmdb_release_date"] or "",
                        "query": candidate,
                        "confidence": confidence,
                        "evidence": evidence,
                        "candidate_score": score_tmdb_match_candidate(features),
                        "features": features.to_dict(),
                    }
                )
                seen.add(key)
                if len(results) >= limit:
                    return results

        return results

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = params or {}
        cache_key = None
        is_search_request = path.startswith("/search/")
        if self.cache.client:
            serialized = urlencode(sorted((str(k), str(v)) for k, v in params.items()))
            cache_prefix = "tmdb:v2" if is_search_request else "tmdb"
            cache_key = f"{cache_prefix}:{path}:{serialized}"
            cached = await self.cache.get_json(cache_key)
            if cached is not None:
                return cached

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        _retryable = {500, 502, 503, 504}
        for attempt in range(3):
            response = await self.client.get(
                f"{self.base}{path}",
                params=params,
                headers=headers,
            )
            if response.status_code in _retryable and attempt < 2:
                self.log.warning("TMDB transient %s for %s, retry %d/2", response.status_code, path, attempt + 1)
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            break
        response.raise_for_status()
        data = response.json()
        cache_ttl = self.cfg.tmdb_cache_ttl
        if is_search_request and not (data.get("results") or []):
            cache_ttl = max(0, int(self.cfg.tmdb_negative_cache_ttl))
        if cache_key and cache_ttl > 0:
            await self.cache.set_json(cache_key, data, ex=cache_ttl)
        return data

    async def ensure_genres(self, force: bool = False) -> None:
        if not self.token:
            return
        last_sync = self.db.get_meta("tmdb_genres_synced_at")
        if not force and last_sync:
            try:
                if utc_ts() - int(last_sync) < 86400:
                    return
            except Exception:
                pass

        for media_type in ("movie", "tv"):
            data = await self._get(f"/genre/{media_type}/list", {"language": self.language})
            genres = {int(g["id"]): g["name"] for g in data.get("genres", [])}
            self.db.upsert_genres(media_type, genres)
        self.db.set_meta("tmdb_genres_synced_at", str(utc_ts()))
        self.log.info("TMDB genres synced")

    async def find_by_imdb(self, imdb_id: str) -> Optional[Dict[str, Any]]:
        if not self.token or not imdb_id:
            return None
        data = await self._get(
            f"/find/{imdb_id}",
            {"external_source": "imdb_id", "language": self.language},
        )
        for bucket, media_type in (("movie_results", "movie"), ("tv_results", "tv")):
            results = data.get(bucket) or []
            if results:
                best = results[0]
                return await self.get_details(media_type, int(best["id"]))
        return None

    async def search_ranked(self, query: str, media_type: str, year: Optional[int], limit: int = 5) -> List[Dict[str, Any]]:
        if not self.token or not query:
            return []

        raw_query = compact_spaces(str(query or "")).strip()
        if looks_like_structured_numeric_title(raw_query):
            query = normalize_structured_numeric_title(raw_query)
            cleaned_query = query
        else:
            query = compact_spaces(clean_release_title(raw_query))
            if not query or is_bad_tmdb_candidate(query):
                return []
            cleaned_query = clean_release_title(query)

        if not query or is_bad_tmdb_candidate(query):
            return []

        query_tokens = set(text_tokens(cleaned_query))
        short_common_query = is_short_or_common_tmdb_query(cleaned_query)
        acronym_query = is_short_acronym_tmdb_query(cleaned_query)
        normalized_query = normalize_match_text(cleaned_query)

        async def fetch_results(lang: str) -> List[Dict[str, Any]]:
            params: Dict[str, Any] = {
                "query": query,
                "language": lang,
                "include_adult": "false",
            }
            if media_type == "movie" and year:
                params["year"] = year
            if media_type == "tv" and year:
                params["first_air_date_year"] = year
            data = await self._get(f"/search/{media_type}", params)
            return data.get("results") or []

        def score_row(row: Dict[str, Any], idx: int, lang: str) -> float:
            title = compact_spaces(row.get("title") or row.get("name") or "")
            original = compact_spaces(row.get("original_title") or row.get("original_name") or "")
            title_clean = clean_release_title(title)
            original_clean = clean_release_title(original)
            score = max(
                similarity(cleaned_query, title),
                similarity(cleaned_query, original),
                similarity(cleaned_query, title_clean),
                similarity(cleaned_query, original_clean),
            )
            low_q = cleaned_query.lower()
            for candidate_value in [title, original, title_clean, original_clean]:
                low_c = (candidate_value or "").lower()
                if low_q and low_c and (low_q in low_c or low_c in low_q):
                    score += 0.12

            candidate_tokens = set()
            for candidate_value in [title, original, title_clean, original_clean]:
                candidate_tokens.update(text_tokens(candidate_value or ""))
            if query_tokens and candidate_tokens:
                overlap = len(query_tokens & candidate_tokens) / max(len(query_tokens), 1)
                score += overlap * 0.22
                if short_common_query and overlap == 0:
                    score -= 0.28

            normalized_candidates = [
                normalize_match_text(candidate_value or "")
                for candidate_value in [title, original, title_clean, original_clean]
                if candidate_value
            ]
            if normalized_query and any(normalized_query == candidate for candidate in normalized_candidates):
                score += 0.18

            score += max(0.0, 0.10 - idx * 0.015)
            if lang != self.language:
                score += 0.05

            date_value = row.get("release_date") or row.get("first_air_date") or ""
            row_year = parse_year(date_value)
            if year and row_year:
                year_delta = abs(row_year - year)
                score -= min(year_delta, 4) * (0.08 if short_common_query else 0.03)
                if media_type == "movie":
                    score -= min(year_delta, 12) * 0.05
                    if year_delta >= 6:
                        score -= 0.20
                    if year_delta >= 10:
                        score -= 0.50
                    if year_delta >= 20:
                        score -= 1.00
                if media_type == "tv":
                    score -= min(year_delta, 8) * (0.06 if short_common_query else 0.025)
                    if year_delta >= 5:
                        score -= 0.22
                    if year_delta >= 10:
                        score -= 0.60
                    if year_delta >= 20:
                        score -= 1.10
                if short_common_query and year_delta >= 2:
                    score -= 0.18 if media_type == "movie" else 0.38
                if acronym_query:
                    score -= min(year_delta, 8) * 0.12
                    if year_delta >= 2:
                        score -= 0.42
                if media_type == "tv" and lang != self.language and year_delta <= 1:
                    score += 0.08
            return score

        min_score = 0.42
        if media_type == "tv" and is_long_latin_tmdb_query(query):
            min_score = 0.30
        if short_common_query:
            min_score = max(min_score, 0.56)
        if acronym_query:
            min_score = max(min_score, 0.70)

        searched_languages: List[str] = [self.language]
        if re.search(r"[A-Za-z]", cleaned_query):
            extra_langs = ["en-US"]
            if media_type == "tv":
                extra_langs.append("ko-KR")
            for lang in extra_langs:
                if lang not in searched_languages:
                    searched_languages.append(lang)

        ranked_rows: List[Tuple[float, int, str, Dict[str, Any]]] = []
        for lang in searched_languages:
            results = await fetch_results(lang)
            for idx, row in enumerate(results[:12]):
                tmdb_id = int(row.get("id") or 0)
                if tmdb_id <= 0:
                    continue
                score = score_row(row, idx, lang)
                candidate_min_score = min_score
                if media_type == "tv" and lang != self.language and re.search(r"[A-Za-z]", query):
                    candidate_min_score = max(0.26, candidate_min_score - 0.04)
                row_year = parse_year(str(row.get("release_date") or row.get("first_air_date") or ""))
                relaxed_long_latin_tv = (
                    media_type == "tv"
                    and is_long_latin_tmdb_query(query)
                    and not acronym_query
                    and idx == 0
                    and not is_bad_tmdb_candidate(compact_spaces(row.get("name") or row.get("title") or "") or compact_spaces(row.get("original_name") or row.get("original_title") or ""))
                    and (not year or not row_year or abs(row_year - year) <= 3)
                )
                if score >= candidate_min_score or relaxed_long_latin_tv:
                    ranked_rows.append((score, idx, lang, row))

        if not ranked_rows:
            return []

        ranked_rows.sort(key=lambda item: (item[0], -item[1], 1 if item[2] == self.language else 0), reverse=True)
        ranked_details: List[Dict[str, Any]] = []
        seen_ids: set[int] = set()
        for score, idx, lang, row in ranked_rows:
            tmdb_id = int(row.get("id") or 0)
            if tmdb_id <= 0 or tmdb_id in seen_ids:
                continue
            details = await self.get_details(media_type, tmdb_id)
            details["search_match_title"] = compact_spaces(row.get("title") or row.get("name") or "") or None
            details["search_match_original_title"] = compact_spaces(row.get("original_title") or row.get("original_name") or "") or None
            details["search_score"] = float(score)
            details["search_rank"] = int(idx)
            details["search_language"] = lang
            details["search_query"] = query
            ranked_details.append(details)
            seen_ids.add(tmdb_id)
            if len(ranked_details) >= max(1, int(limit or 1)):
                break
        return ranked_details

    async def search(self, query: str, media_type: str, year: Optional[int]) -> Optional[Dict[str, Any]]:
        ranked = await self.search_ranked(query, media_type, year, limit=1)
        return ranked[0] if ranked else None

    async def get_details(self, media_type: str, tmdb_id: int) -> Dict[str, Any]:
        append_parts = ["external_ids"]
        if media_type == "tv":
            append_parts.append("content_ratings")
        else:
            append_parts.append("release_dates")

        data = await self._get(
            f"/{media_type}/{tmdb_id}",
            {
                "language": self.language,
                "append_to_response": ",".join(append_parts),
            },
        )

        def pick_age_rating(payload: Dict[str, Any], mt: str) -> Optional[str]:
            try:
                if mt == "tv":
                    results = (payload.get("content_ratings") or {}).get("results") or []
                    for country in ("RU", "US", "GB"):
                        for row in results:
                            if row.get("iso_3166_1") == country and row.get("rating"):
                                return str(row["rating"]).strip()
                    for row in results:
                        if row.get("rating"):
                            return str(row["rating"]).strip()
                else:
                    results = (payload.get("release_dates") or {}).get("results") or []
                    for country in ("RU", "US", "GB"):
                        for row in results:
                            if row.get("iso_3166_1") == country:
                                for rel in row.get("release_dates") or []:
                                    cert = rel.get("certification")
                                    if cert:
                                        return str(cert).strip()
                    for row in results:
                        for rel in row.get("release_dates") or []:
                            cert = rel.get("certification")
                            if cert:
                                return str(cert).strip()
            except Exception:
                return None
            return None

        def unpack_episode(ep: Optional[Dict[str, Any]], prefix: str) -> Dict[str, Any]:
            if not ep:
                return {
                    f"{prefix}_name": None,
                    f"{prefix}_air_date": None,
                    f"{prefix}_season_number": None,
                    f"{prefix}_episode_number": None,
                }
            return {
                f"{prefix}_name": compact_spaces(ep.get("name") or "") or None,
                f"{prefix}_air_date": ep.get("air_date") or None,
                f"{prefix}_season_number": ep.get("season_number"),
                f"{prefix}_episode_number": ep.get("episode_number"),
            }

        genre_ids = [int(g["id"]) for g in data.get("genres", [])]
        title = data.get("title") or data.get("name") or ""
        original = data.get("original_title") or data.get("original_name") or ""
        release_date = data.get("release_date") or data.get("first_air_date") or ""
        poster_path = data.get("poster_path") or ""
        poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None
        imdb_id = None
        ext = data.get("external_ids") or {}
        if ext.get("imdb_id"):
            imdb_id = ext["imdb_id"]

        countries: List[str] = []
        if media_type == "tv":
            countries = [str(x).strip() for x in (data.get("origin_country") or []) if str(x).strip()]
        else:
            countries = [str((x or {}).get("iso_3166_1") or "").strip() for x in (data.get("production_countries") or []) if str((x or {}).get("iso_3166_1") or "").strip()]

        result = {
            "tmdb_id": int(data["id"]),
            "media_type": media_type,
            "tmdb_title": title,
            "tmdb_original_title": original,
            "tmdb_original_language": normalize_tmdb_language(data.get("original_language")),
            "tmdb_rating": float(data.get("vote_average") or 0.0),
            "tmdb_vote_count": int(data.get("vote_count") or 0),
            "tmdb_release_date": release_date or None,
            "tmdb_overview": compact_spaces(data.get("overview") or ""),
            "tmdb_poster_url": poster_url,
            "tmdb_status": compact_spaces(data.get("status") or "") or None,
            "tmdb_age_rating": pick_age_rating(data, media_type),
            "tmdb_countries": countries,
            "genre_ids": genre_ids,
            "imdb_id": imdb_id,
        }

        if media_type == "tv":
            result.update({
                "tmdb_number_of_seasons": data.get("number_of_seasons"),
                "tmdb_number_of_episodes": data.get("number_of_episodes"),
            })
            result.update(unpack_episode(data.get("next_episode_to_air"), "tmdb_next_episode"))
            result.update(unpack_episode(data.get("last_episode_to_air"), "tmdb_last_episode"))

        return result

    async def enrich_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        resolver_result = None
        lexicon_best = None
        tmdb_match_path = None
        lexicon_candidates_used = False
        fallback_cleaned_title = compact_spaces(item.get("cleaned_title") or "")
        if not fallback_cleaned_title:
            fallback_cleaned_title = _fallback_cleaned_title_from_source_title(item.get("source_title") or "")
            if fallback_cleaned_title:
                item["cleaned_title"] = fallback_cleaned_title
        item["tmdb_match_path"] = None
        item["tmdb_match_confidence"] = compact_spaces(str(item.get("tmdb_match_confidence") or ""))
        item["tmdb_match_evidence"] = compact_spaces(str(item.get("tmdb_match_evidence") or ""))
        item["tmdb_match_debug"] = compact_spaces(str(item.get("tmdb_match_debug") or ""))
        item["_tmdb_match_debug_events"] = []
        if not item.get("media_type"):
            forced = source_category_forced_media_type(
                item.get("source_category_id"), item.get("source_category_name"),
            )
            if forced and forced != "other":
                item["media_type"] = forced
        if self.anime_title_lexicon and should_use_anime_resolver(item):
            try:
                lexicon_candidates = []

                def _push_lex(value):
                    value = " ".join(str(value or "").split()).strip()
                    if value and value not in lexicon_candidates:
                        lexicon_candidates.append(value)

                _push_lex(fallback_cleaned_title)
                _push_lex(item.get("source_title"))

                for raw in [fallback_cleaned_title, item.get("source_title")]:
                    raw = str(raw or "")
                    if "/" in raw:
                        for part in raw.split("/"):
                            _push_lex(part)
                    for paren in re.findall(r"\(([^()]+)\)", raw):
                        _push_lex(paren)
                    stripped = re.sub(r"\([^()]*\)", " ", raw)
                    _push_lex(stripped)

                lexicon_year = None
                raw_year = str(item.get("source_year") or "").strip()
                if raw_year[:4].isdigit():
                    lexicon_year = int(raw_year[:4])

                lexicon_best = self.anime_title_lexicon.find_best(lexicon_candidates, year=lexicon_year)
                if lexicon_best:
                    logging.getLogger(__name__).info(
                        "Anime lexicon best canonical=%s media=%s year=%s source=%s aliases=%s",
                        lexicon_best.canonical_title,
                        lexicon_best.media_type,
                        lexicon_best.year,
                        lexicon_best.source,
                        ", ".join(lexicon_best.titles[:6]),
                    )
                    for _src in (lexicon_best.raw or {}).get("sources") or []:
                        _mal = re.search(r"myanimelist\.net/anime/(\d+)", str(_src))
                        if _mal:
                            item["mal_id"] = _mal.group(1)
                            break
                else:
                    logging.getLogger(__name__).info(
                        "Anime lexicon miss title=%s cleaned=%s",
                        item.get("source_title") or "",
                        fallback_cleaned_title or "",
                    )
            except Exception:
                logging.getLogger(__name__).exception(
                    "Anime lexicon log-only probe failed title=%s",
                    item.get("source_title") or "",
                )

        if self.anime_mapping_store and should_use_anime_resolver(item):
            try:
                resolver_result = resolve_anime_tmdb(item, self.anime_mapping_store)
                if resolver_result:
                    logging.getLogger(__name__).info(
                        "Anime resolver hit source=%s tmdb_id=%s media=%s title=%s confidence=%s",
                        resolver_result.get("resolver_source"),
                        resolver_result.get("tmdb_id"),
                        resolver_result.get("media_type"),
                        resolver_result.get("resolver_matched_title"),
                        resolver_result.get("resolver_confidence"),
                    )
                else:
                    logging.getLogger(__name__).info(
                        "Anime resolver miss title=%s cleaned=%s",
                        item.get("source_title") or "",
                        fallback_cleaned_title or "",
                    )
            except Exception:
                logging.getLogger(__name__).exception(
                    "Anime resolver log-only probe failed title=%s",
                    item.get("source_title") or "",
                )
                resolver_result = None

        if not self.token:
            self._finalize_match_debug(item)
            return item

        source_text = f"{item.get('source_title') or ''} {item.get('source_description') or ''}"
        if item.get("media_type") == "other" or is_non_video_release(source_text):
            self._finalize_match_debug(item)
            return item
        if should_skip_tmdb_lookup(item):
            self.log.info(
                "TMDB skipped by source category/title heuristics for %s category=%s",
                item.get("source_title"),
                item.get("source_category_name"),
            )
            self._record_match_debug(item, "search_skipped", reason="category_or_title_heuristic")
            self._finalize_match_debug(item)
            return item

        try:
            source_imdb_id = item.get("source_imdb_id")
            details = None
            terminal_override = False
            stored_override = self._kinozal_override(item)
            if stored_override:
                terminal_override = True
                try:
                    override_details = await self.get_details(
                        str(stored_override.get("media_type") or "movie"),
                        int(stored_override["tmdb_id"]),
                    )
                    if override_details:
                        details = override_details
                        terminal_override = True
                        self._apply_match_metadata(
                            item,
                            details,
                            path="stored_override",
                            confidence="verified",
                            evidence=f"kinozal_id override source={compact_spaces(str(stored_override.get('source') or 'admin'))}",
                        )
                except Exception:
                    self.log.warning(
                        "TMDB stored override failed for %s -> %s",
                        item.get("source_title"),
                        stored_override,
                        exc_info=True,
                    )

            override = manual_tmdb_override_for_item(item)
            if override and not details and not stored_override:
                terminal_override = True
                override_media_type, override_tmdb_id, override_key = override
                try:
                    override_details = await self.get_details(override_media_type, int(override_tmdb_id))
                    if override_details:
                        terminal_override = True
                        override_details["search_match_title"] = override_key
                        override_details["search_match_original_title"] = override_key
                        details = override_details
                        tmdb_match_path = "manual_override"
                        self._apply_match_metadata(
                            item,
                            details,
                            path=tmdb_match_path,
                            confidence="verified",
                            evidence=f"static override key={override_key}",
                        )
                        self.log.info(
                            "TMDB manual override matched %s -> %s [%s:%s]",
                            item.get("source_title"),
                            override_key,
                            override_media_type,
                            override_tmdb_id,
                        )
                except Exception:
                    self.log.warning(
                        "TMDB manual override failed for %s -> %s [%s:%s]",
                        item.get("source_title"),
                        override_key,
                        override_media_type,
                        override_tmdb_id,
                        exc_info=True,
                    )
            if source_imdb_id and not details and not terminal_override:
                details = await self.find_by_imdb(source_imdb_id)
                if details:
                    tmdb_match_path = "imdb_lookup"
                    if self._is_rejected_match(item, details):
                        self.log.info(
                            "TMDB skipped rejected IMDb lookup for %s -> tmdb_id=%s",
                            item.get("source_title"),
                            details.get("tmdb_id"),
                        )
                        details = None
                    else:
                        self._apply_match_metadata(
                            item,
                            details,
                            path=tmdb_match_path,
                            confidence="verified",
                            evidence=f"source_imdb_id={source_imdb_id}",
                        )

            if (
                not terminal_override
                and
                self.cfg.anime_resolver_enabled
                and resolver_result
                and resolver_result.get("resolver_confidence") == "high"
            ):
                try:
                    resolver_media_type = str(
                        resolver_result.get("media_type")
                        or item.get("media_type")
                        or "tv"
                    ).strip().lower() or "tv"

                    item_media_type = str(item.get("media_type") or "").strip().lower()

                    current_tmdb_id = None
                    try:
                        current_tmdb_id = int(item.get("tmdb_id")) if item.get("tmdb_id") is not None else None
                    except Exception:
                        current_tmdb_id = None

                    resolver_tmdb_id = int(resolver_result["tmdb_id"])

                    if item_media_type in {"tv", "movie"} and resolver_media_type != item_media_type:
                        self.log.info(
                            "Anime resolver skipped due media mismatch title=%s item_media=%s resolver_media=%s",
                            item.get("source_title"),
                            item_media_type,
                            resolver_media_type,
                        )
                    elif not details or current_tmdb_id != resolver_tmdb_id:
                        resolved_details = await self.get_details(
                            resolver_media_type,
                            resolver_tmdb_id,
                        )
                        if resolved_details:
                            matched_title = resolver_result.get("resolver_matched_title") or ""
                            resolved_details["search_match_title"] = matched_title or None
                            resolved_details["search_match_original_title"] = matched_title or None
                            if self._is_rejected_match(item, resolved_details):
                                self.log.info(
                                    "Anime resolver rejected by memory for %s -> tmdb_id=%s",
                                    item.get("source_title"),
                                    resolved_details.get("tmdb_id"),
                                )
                                details = None
                            else:
                                tmdb_match_path = "anime_resolver_direct"
                                self._apply_match_metadata(
                                    item,
                                    resolved_details,
                                    path=tmdb_match_path,
                                    confidence="high",
                                    evidence=(
                                        "anime resolver "
                                        f"source={resolver_result.get('resolver_source')} "
                                        f"confidence={resolver_result.get('resolver_confidence')}"
                                    ),
                                )
                                details = resolved_details
                                self.log.info(
                                    "Anime resolver adopted %s -> tmdb_id=%s [%s] source=%s confidence=%s",
                                    item.get("source_title"),
                                    resolver_result.get("tmdb_id"),
                                    resolver_media_type,
                                    resolver_result.get("resolver_source"),
                                    resolver_result.get("resolver_confidence"),
                                )
                except Exception:
                    self.log.warning(
                        "Anime resolver direct lookup failed for %s -> tmdb_id=%s",
                        item.get("source_title"),
                        resolver_result.get("tmdb_id") if resolver_result else None,
                        exc_info=True,
                    )

            if not details and not terminal_override:
                media_type = item.get("media_type") or "movie"
                year = item.get("source_year")
                candidates = title_search_candidates(
                    item.get("source_title") or "",
                    fallback_cleaned_title or "",
                )

                extra_slash_candidates = _extract_slash_title_candidates(item.get("source_title") or "")
                if extra_slash_candidates:
                    candidates = extra_slash_candidates + [
                        x for x in candidates if x not in extra_slash_candidates
                    ]

                if lexicon_best:
                    if _should_skip_generic_lexicon_expansion(item, lexicon_best):
                        logging.getLogger(__name__).info(
                            "Anime lexicon weak alias hit ignored title=%s canonical=%s reason=%s",
                            item.get("source_title") or "",
                            lexicon_best.canonical_title,
                            "generic_single_token_alias",
                        )
                    else:
                        expanded_candidates = []
                        for value in lexicon_best.titles[:8]:
                            value = " ".join(str(value or "").split()).strip()
                            if (
                                value
                                and value not in expanded_candidates
                                and value not in candidates
                                and _should_expand_lexicon_alias(item, fallback_cleaned_title, lexicon_best, value)
                            ):
                                expanded_candidates.append(value)

                        if expanded_candidates:
                            logging.getLogger(__name__).info(
                                "Anime lexicon expanded candidates title=%s canonical=%s added=%s",
                                item.get("source_title") or "",
                                lexicon_best.canonical_title,
                                " | ".join(expanded_candidates),
                            )
                            candidates = expanded_candidates + candidates

                raw_candidates = list(candidates)
                candidates = [
                    candidate for candidate in candidates
                    if not _should_skip_generic_search_candidate(item, candidate)
                ]
                filtered_out_candidates = [candidate for candidate in raw_candidates if candidate not in candidates]
                if filtered_out_candidates:
                    self._record_match_debug(
                        item,
                        "filtered_candidates",
                        candidates=filtered_out_candidates[:8],
                        reason="generic_candidate_filtered",
                    )
                if raw_candidates and not candidates:
                    self.log.info(
                        "TMDB all candidates filtered as too generic for %s | raw_candidates=%s",
                        item.get("source_title"),
                        raw_candidates,
                    )
                search_plan = self._build_search_plan(item, candidates, media_type, year)
                self._record_match_debug(
                    item,
                    "search_plan",
                    queries=[
                        {
                            "query": candidate,
                            "media_type": mt,
                            "year": y,
                        }
                        for candidate, mt, y in search_plan[:20]
                    ],
                )

                seen = set()
                candidate_pool: List[Dict[str, Any]] = []
                pooled_by_key: Dict[Tuple[str, int], Dict[str, Any]] = {}
                for candidate, mt, y in search_plan:
                    if _is_hard_blocked_generic_candidate(candidate):
                        self.log.info(
                            "TMDB hard-skipped generic candidate for %s -> %s",
                            item.get("source_title"),
                            candidate,
                        )
                        self._record_match_debug(
                            item,
                            "skipped_candidate",
                            query=candidate,
                            media_type=mt,
                            year=y,
                            reason="hard_blocked_generic_candidate",
                        )
                        continue

                    key = (candidate.lower(), mt, y)
                    if key in seen:
                        continue
                    seen.add(key)
                    ranked_details = await self.search_ranked(candidate, mt, y, limit=5)
                    if not ranked_details:
                        self._record_match_debug(
                            item,
                            "query_no_results",
                            query=candidate,
                            media_type=mt,
                            year=y,
                        )
                        continue

                    for ranked_details_item in ranked_details:
                        features = extract_tmdb_match_features(item, candidate, ranked_details_item, mt)
                        candidate_score = score_tmdb_match_candidate(features)
                        self._record_match_debug(
                            item,
                            "candidate_probe",
                            query=candidate,
                            media_type=mt,
                            requested_year=y,
                            tmdb_id=ranked_details_item.get("tmdb_id"),
                            tmdb_title=ranked_details_item.get("tmdb_title") or ranked_details_item.get("tmdb_original_title"),
                            tmdb_year=parse_year(str(ranked_details_item.get("tmdb_release_date") or "")),
                            score=ranked_details_item.get("search_score"),
                            candidate_score=candidate_score,
                            features=features.to_dict(),
                        )
                        pool_key = (str(ranked_details_item.get("media_type") or mt), int(ranked_details_item.get("tmdb_id") or 0))
                        candidate_entry = {
                            "query": candidate,
                            "requested_media_type": mt,
                            "requested_year": y,
                            "details": ranked_details_item,
                            "features": features,
                            "candidate_score": candidate_score,
                            "search_score": float(ranked_details_item.get("search_score") or 0.0),
                        }
                        existing_entry = pooled_by_key.get(pool_key)
                        if existing_entry is None or (
                            candidate_score,
                            candidate_entry["search_score"],
                        ) > (
                            float(existing_entry.get("candidate_score") or 0.0),
                            float(existing_entry.get("search_score") or 0.0),
                        ):
                            pooled_by_key[pool_key] = candidate_entry

                candidate_pool = sorted(
                    pooled_by_key.values(),
                    key=lambda row: (
                        float(row.get("candidate_score") or 0.0),
                        float(row.get("search_score") or 0.0),
                    ),
                    reverse=True,
                )

                if candidate_pool:
                    self._record_match_debug(
                        item,
                        "candidate_ranking",
                        candidates=[
                            {
                                "query": str(entry.get("query") or ""),
                                "media_type": str(entry.get("requested_media_type") or ""),
                                "tmdb_id": int(entry["details"].get("tmdb_id") or 0),
                                "tmdb_title": entry["details"].get("tmdb_title") or entry["details"].get("tmdb_original_title") or "",
                                "candidate_score": round(float(entry.get("candidate_score") or 0.0), 3),
                                "search_score": round(float(entry.get("search_score") or 0.0), 3),
                            }
                            for entry in candidate_pool[:12]
                        ],
                    )

                for candidate_entry in candidate_pool:
                    candidate = str(candidate_entry.get("query") or "")
                    mt = str(candidate_entry.get("requested_media_type") or "")
                    y = candidate_entry.get("requested_year")
                    ranked_details_item = candidate_entry["details"]
                    features = candidate_entry["features"]

                    if not tmdb_match_looks_valid(item, candidate, ranked_details_item, mt):
                        reject_reason = compact_spaces(str(ranked_details_item.get("tmdb_validation_reject_reason") or "validation_reject"))
                        reject_code = compact_spaces(
                            str(
                                ranked_details_item.get("tmdb_validation_reject_code")
                                or tmdb_validation_reason_code(reject_reason)
                            )
                        )
                        self.log.info(
                            "TMDB rejected suspicious match for %s -> %s [%s / %s] code=%s reason=%s",
                            item.get("source_title"),
                            candidate,
                            ranked_details_item.get("tmdb_title"),
                            ranked_details_item.get("tmdb_original_title"),
                            reject_code,
                            reject_reason,
                        )
                        self._record_match_debug(
                            item,
                            "candidate_rejected",
                            query=candidate,
                            media_type=mt,
                            requested_year=y,
                            tmdb_id=ranked_details_item.get("tmdb_id"),
                            tmdb_title=ranked_details_item.get("tmdb_title") or ranked_details_item.get("tmdb_original_title"),
                            reason_code=reject_code,
                            reason=reject_reason,
                            warnings=ranked_details_item.get("tmdb_validation_warnings") or [],
                            candidate_score=candidate_entry.get("candidate_score"),
                            features=features.to_dict(),
                        )
                        continue

                    if self._is_rejected_match(item, ranked_details_item):
                        self.log.info(
                            "TMDB search match rejected by memory for %s -> %s [tmdb_id=%s]",
                            item.get("source_title"),
                            candidate,
                            ranked_details_item.get("tmdb_id"),
                        )
                        self._record_match_debug(
                            item,
                            "candidate_rejected",
                            query=candidate,
                            media_type=mt,
                            requested_year=y,
                            tmdb_id=ranked_details_item.get("tmdb_id"),
                            tmdb_title=ranked_details_item.get("tmdb_title") or ranked_details_item.get("tmdb_original_title"),
                            reason="memory_rejected_match",
                            candidate_score=candidate_entry.get("candidate_score"),
                            features=features.to_dict(),
                        )
                        continue

                    if (ranked_details_item.get("media_type") or mt) == "movie" and not item.get("source_imdb_id"):
                        source_year = parse_year(str(item.get("source_year") or ""))
                        matched_year = parse_year(str(ranked_details_item.get("tmdb_release_date") or ""))
                        if source_year and matched_year and abs(source_year - matched_year) > 2:
                            self.log.info(
                                "TMDB rejected movie search match by year delta for %s -> %s [source_year=%s tmdb_year=%s]",
                                item.get("source_title"),
                                candidate,
                                source_year,
                                matched_year,
                            )
                            self._record_match_debug(
                                item,
                                "candidate_rejected",
                                query=candidate,
                                media_type=mt,
                                requested_year=y,
                                tmdb_id=ranked_details_item.get("tmdb_id"),
                                tmdb_title=ranked_details_item.get("tmdb_title") or ranked_details_item.get("tmdb_original_title"),
                                reason="movie_year_mismatch",
                                source_year=source_year,
                                tmdb_year=matched_year,
                                candidate_score=candidate_entry.get("candidate_score"),
                                features=features.to_dict(),
                            )
                            continue

                    details = ranked_details_item
                    matched_media_type = details.get("media_type") or mt
                    matched_year = parse_year(str(details.get("tmdb_release_date") or ""))
                    tmdb_match_path = "search"
                    confidence, evidence = _search_match_confidence(item, details)
                    warnings = details.get("tmdb_validation_warnings") or []
                    warning_suffix = f"; warnings={','.join(str(value) for value in warnings)}" if warnings else ""
                    self._apply_match_metadata(
                        item,
                        details,
                        path=tmdb_match_path,
                        confidence=confidence,
                        evidence=(
                            f"query={candidate}; media={matched_media_type}; scorer={float(candidate_entry.get('candidate_score') or 0.0):.3f}; "
                            f"{evidence}{warning_suffix}"
                        ),
                    )
                    self._record_match_debug(
                        item,
                        "candidate_accepted",
                        query=candidate,
                        media_type=matched_media_type,
                        requested_year=y,
                        tmdb_id=details.get("tmdb_id"),
                        tmdb_title=details.get("tmdb_title") or details.get("tmdb_original_title"),
                        tmdb_year=matched_year,
                        warnings=warnings,
                        candidate_score=candidate_entry.get("candidate_score"),
                        features=features.to_dict(),
                    )
                    self.log.info(
                        "TMDB matched %s -> %s [%s, tmdb_year=%s, tmdb_id=%s]",
                        item.get("source_title"),
                        candidate,
                        matched_media_type,
                        matched_year,
                        details.get("tmdb_id"),
                    )
                    break

                if not details:
                    if not candidates:
                        self.log.info("TMDB no search candidates extracted for %s", item.get("source_title"))
                        self._record_match_debug(item, "search_failed", reason="no_search_candidates")
                    self.log.info("TMDB no match for %s | candidates=%s", item.get("source_title"), candidates)

            if details:
                item.update(details)
                if not item.get("media_type"):
                    item["media_type"] = details.get("media_type")
                if not item.get("imdb_id"):
                    item["imdb_id"] = details.get("imdb_id")
            elif item.get("tmdb_id") is None and not terminal_override:
                item["tmdb_match_confidence"] = "unmatched"
                if not compact_spaces(str(item.get("tmdb_match_path") or "")):
                    item["tmdb_match_path"] = "search_unmatched"
                if not compact_spaces(str(item.get("tmdb_match_evidence") or "")):
                    item["tmdb_match_evidence"] = "no valid TMDB candidate; see tmdb_match_debug"
            self._finalize_match_debug(item)
            return item
        except Exception:
            self.log.exception("TMDB enrichment failed for %s", item.get("source_title"))
            self._record_match_debug(item, "search_failed", reason="tmdb_enrichment_exception")
            self._finalize_match_debug(item)
            return item
