import logging
import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx

from country_helpers import normalize_tmdb_language
from match_text import similarity, normalize_match_text, text_tokens
from media_detection import is_non_video_release
from parsing_basic import parse_year
from title_prep import clean_release_title, looks_like_structured_numeric_title, normalize_structured_numeric_title, should_skip_tmdb_lookup, is_bad_tmdb_candidate
from tmdb_aliases import is_long_latin_tmdb_query, is_short_or_common_tmdb_query, is_short_acronym_tmdb_query, manual_tmdb_override_for_item, title_search_candidates
from tmdb_match_validation import tmdb_match_looks_valid
from utils import utc_ts, compact_spaces


class TMDBClient:
    def __init__(self, cfg: Any, db: Any, cache: Any, token: str, language: str, log: logging.Logger):
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
        response = await self.client.get(
            f"{self.base}{path}",
            params=params,
            headers=headers,
        )
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

    async def search(self, query: str, media_type: str, year: Optional[int]) -> Optional[Dict[str, Any]]:
        if not self.token or not query:
            return None

        raw_query = compact_spaces(str(query or "")).strip()
        if looks_like_structured_numeric_title(raw_query):
            query = normalize_structured_numeric_title(raw_query)
            cleaned_query = query
        else:
            query = compact_spaces(clean_release_title(raw_query))
            if not query or is_bad_tmdb_candidate(query):
                return None
            cleaned_query = clean_release_title(query)

        if not query or is_bad_tmdb_candidate(query):
            return None
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

        async def evaluate_results(results: List[Dict[str, Any]], lang: str) -> Optional[Dict[str, Any]]:
            if not results:
                return None

            exact_matches = []
            for idx, row in enumerate(results[:12]):
                title = compact_spaces(row.get("title") or row.get("name") or "")
                original = compact_spaces(row.get("original_title") or row.get("original_name") or "")
                variants = [
                    title,
                    original,
                    clean_release_title(title),
                    clean_release_title(original),
                ]
                normalized_variants = [normalize_match_text(v) for v in variants if v]
                if normalized_query and any(normalized_query == v for v in normalized_variants):
                    row_year = parse_year(str(row.get("release_date") or row.get("first_air_date") or ""))
                    year_delta = abs(row_year - year) if year and row_year else 9999
                    exact_matches.append((year_delta, idx, row_year or 0, row))

            if exact_matches and (acronym_query or short_common_query):
                exact_matches.sort(key=lambda x: (x[0], x[1]))
                best_year_delta, best_idx, _best_item_year, best_row = exact_matches[0]
                relaxed_year_limit = 2 if acronym_query else 5
                if best_year_delta <= relaxed_year_limit or (year is None and best_idx == 0):
                    details = await self.get_details(media_type, int(best_row["id"]))
                    details["search_match_title"] = compact_spaces(best_row.get("title") or best_row.get("name") or "") or None
                    details["search_match_original_title"] = compact_spaces(best_row.get("original_title") or best_row.get("original_name") or "") or None
                    return details

            best_score = -1.0
            best_id = None
            best_rank = 999

            for idx, item in enumerate(results[:12]):
                title = compact_spaces(item.get("title") or item.get("name") or "")
                original = compact_spaces(item.get("original_title") or item.get("original_name") or "")
                title_clean = clean_release_title(title)
                original_clean = clean_release_title(original)
                score = max(
                    similarity(cleaned_query, title),
                    similarity(cleaned_query, original),
                    similarity(cleaned_query, title_clean),
                    similarity(cleaned_query, original_clean),
                )
                low_q = cleaned_query.lower()
                for cand in [title, original, title_clean, original_clean]:
                    low_c = (cand or "").lower()
                    if low_q and low_c and (low_q in low_c or low_c in low_q):
                        score += 0.12

                candidate_tokens = set()
                for cand in [title, original, title_clean, original_clean]:
                    candidate_tokens.update(text_tokens(cand or ""))
                overlap = 0.0
                if query_tokens and candidate_tokens:
                    overlap = len(query_tokens & candidate_tokens) / max(len(query_tokens), 1)
                    score += overlap * 0.22
                    if short_common_query and overlap == 0:
                        score -= 0.28

                normalized_candidates = [normalize_match_text(cand or "") for cand in [title, original, title_clean, original_clean] if cand]
                if normalized_query and any(normalized_query == cand for cand in normalized_candidates):
                    score += 0.18

                score += max(0.0, 0.10 - idx * 0.015)
                if lang != self.language:
                    score += 0.05

                date_value = item.get("release_date") or item.get("first_air_date") or ""
                item_year = parse_year(date_value)
                if year and item_year:
                    year_delta = abs(item_year - year)
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

                if score > best_score:
                    best_score = score
                    best_id = int(item["id"])
                    best_rank = idx

            min_score = 0.42
            if media_type == "tv" and is_long_latin_tmdb_query(query):
                min_score = 0.30
            if short_common_query:
                min_score = max(min_score, 0.56)
            if acronym_query:
                min_score = max(min_score, 0.70)
            if media_type == "tv" and lang != self.language and re.search(r"[A-Za-z]", query):
                min_score = max(0.26, min_score - 0.04)

            if best_id is not None and best_score >= min_score:
                details = await self.get_details(media_type, best_id)
                matched = next((row for row in results if int(row.get("id") or 0) == int(best_id)), None)
                if matched:
                    details["search_match_title"] = compact_spaces(matched.get("title") or matched.get("name") or "") or None
                    details["search_match_original_title"] = compact_spaces(matched.get("original_title") or matched.get("original_name") or "") or None
                return details

            if media_type == "tv" and is_long_latin_tmdb_query(query) and not acronym_query:
                top = results[0]
                top_title = compact_spaces(top.get("name") or top.get("title") or "")
                top_original = compact_spaces(top.get("original_name") or top.get("original_title") or "")
                top_year = parse_year(str(top.get("first_air_date") or top.get("release_date") or ""))
                if (top_title or top_original) and not is_bad_tmdb_candidate(top_title or top_original):
                    if not year or not top_year or abs(top_year - year) <= 3:
                        self.log.info(
                            "TMDB relaxed match accepted for query=%s lang=%s -> %s / %s [rank=%s, score=%.3f]",
                            query,
                            lang,
                            top_title,
                            top_original,
                            best_rank,
                            best_score,
                        )
                        details = await self.get_details(media_type, int(top["id"]))
                        details["search_match_title"] = compact_spaces(top.get("name") or top.get("title") or "") or None
                        details["search_match_original_title"] = compact_spaces(top.get("original_name") or top.get("original_title") or "") or None
                        return details

            return None

        searched_languages: List[str] = [self.language]
        if re.search(r"[A-Za-z]", cleaned_query):
            extra_langs = ["en-US"]
            if media_type == "tv":
                extra_langs.append("ko-KR")
            for lang in extra_langs:
                if lang not in searched_languages:
                    searched_languages.append(lang)

        last_results: List[Dict[str, Any]] = []
        for lang in searched_languages:
            results = await fetch_results(lang)
            last_results = results or last_results
            details = await evaluate_results(results, lang)
            if details is not None:
                return details

        return None

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
        if not self.token:
            return item

        source_text = f"{item.get('source_title') or ''} {item.get('source_description') or ''}"
        if item.get("media_type") == "other" or is_non_video_release(source_text):
            return item
        if should_skip_tmdb_lookup(item):
            self.log.info(
                "TMDB skipped by source category/title heuristics for %s category=%s",
                item.get("source_title"),
                item.get("source_category_name"),
            )
            return item

        try:
            imdb_id = item.get("imdb_id")
            details = None
            override = manual_tmdb_override_for_item(item)
            if override:
                override_media_type, override_tmdb_id, override_key = override
                try:
                    override_details = await self.get_details(override_media_type, int(override_tmdb_id))
                    if override_details:
                        override_details["search_match_title"] = override_key
                        override_details["search_match_original_title"] = override_key
                        details = override_details
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
            if imdb_id and not details:
                details = await self.find_by_imdb(imdb_id)

            if not details:
                media_type = item.get("media_type") or "movie"
                year = item.get("source_year")
                candidates = title_search_candidates(
                    item.get("source_title") or "",
                    item.get("cleaned_title") or "",
                )

                search_plan: List[Tuple[str, str, Optional[int]]] = []
                strict_tv_only = bool(item.get("source_episode_progress")) or media_type == "tv"
                if media_type == "tv":
                    for candidate in candidates:
                        search_plan.extend([
                            (candidate, "tv", year),
                            (candidate, "tv", None),
                        ])
                        if not strict_tv_only:
                            search_plan.extend([
                                (candidate, "movie", year),
                                (candidate, "movie", None),
                            ])
                else:
                    for candidate in candidates:
                        search_plan.extend([
                            (candidate, "movie", year),
                            (candidate, "movie", None),
                            (candidate, "tv", None),
                            (candidate, "tv", year),
                        ])

                seen = set()
                for candidate, mt, y in search_plan:
                    key = (candidate.lower(), mt, y)
                    if key in seen:
                        continue
                    seen.add(key)
                    details = await self.search(candidate, mt, y)
                    if details and not tmdb_match_looks_valid(item, candidate, details, mt):
                        self.log.info(
                            "TMDB rejected suspicious match for %s -> %s [%s / %s]",
                            item.get("source_title"),
                            candidate,
                            details.get("tmdb_title"),
                            details.get("tmdb_original_title"),
                        )
                        details = None
                    if details:
                        matched_media_type = details.get("media_type") or mt
                        matched_year = parse_year(str(details.get("tmdb_release_date") or ""))
                        self.log.info("TMDB matched %s -> %s [%s, tmdb_year=%s]", item.get("source_title"), candidate, matched_media_type, matched_year)
                        break

                if not details:
                    if not candidates:
                        self.log.info("TMDB no search candidates extracted for %s", item.get("source_title"))
                    self.log.info("TMDB no match for %s | candidates=%s", item.get("source_title"), candidates)

            if details:
                item.update(details)
                if not item.get("media_type"):
                    item["media_type"] = details.get("media_type")
                if not item.get("imdb_id"):
                    item["imdb_id"] = details.get("imdb_id")
            return item
        except Exception:
            self.log.exception("TMDB enrichment failed for %s", item.get("source_title"))
            return item
