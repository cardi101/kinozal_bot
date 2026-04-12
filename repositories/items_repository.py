import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from country_helpers import parse_country_codes
from release_versioning import (
    build_item_variant_signature,
    build_version_signature,
    extract_kinozal_id,
    format_variant_summary,
    get_item_variant_components,
    resolve_item_kinozal_id,
)
from subscription_matching import match_subscription
from utils import compact_spaces, utc_ts

from .base import BaseRepository

log = logging.getLogger("kinozal-news-bot")


def item_duplicate_quality_score(item: Dict[str, Any]) -> int:
    score = 0
    if item.get("tmdb_id"):
        score += 100
    if compact_spaces(str(item.get("imdb_id") or "")):
        score += 25
    if compact_spaces(str(item.get("tmdb_overview") or "")):
        score += 10
    if compact_spaces(str(item.get("source_description") or "")):
        score += 8
    if compact_spaces(str(item.get("tmdb_poster_url") or "")):
        score += 5
    if parse_country_codes(item.get("tmdb_countries")):
        score += 4
    if compact_spaces(str(item.get("tmdb_status") or "")):
        score += 3
    if compact_spaces(str(item.get("manual_bucket") or "")):
        score += 2
    if int(item.get("tmdb_vote_count") or 0) > 0:
        score += 2
    return score


class ItemsRepository(BaseRepository):
    def find_existing_enriched(self, source_uid: str, source_title: str) -> Optional[Dict[str, Any]]:
        source_uid = compact_spaces(str(source_uid or ""))
        source_title = compact_spaces(str(source_title or ""))
        if not source_uid or not source_title:
            return None
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM items WHERE source_uid = ? AND source_title = ? AND tmdb_id IS NOT NULL ORDER BY created_at DESC LIMIT 1",
                (source_uid, source_title),
            ).fetchone()
            return dict(row) if row else None

    def _find_existing_item_for_upsert(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM items WHERE source_uid = ? AND version_signature = ?",
            (data["source_uid"], data["version_signature"]),
        ).fetchone()
        if row:
            return row

        kinozal_id = compact_spaces(str(data.get("kinozal_id") or "")) or extract_kinozal_id(data.get("source_uid")) or extract_kinozal_id(data.get("source_link"))
        target_variant_sig = build_item_variant_signature(data)

        if kinozal_id:
            source_uid_norm = f"kinozal:{kinozal_id}"
            like_pattern = f"%details.php?id={kinozal_id}%"
            rows = self.conn.execute(
                """
                SELECT * FROM items
                WHERE source_uid = ? OR source_uid LIKE ? OR source_link LIKE ?
                ORDER BY CASE WHEN source_uid = ? THEN 0 ELSE 1 END, created_at DESC
                """,
                (source_uid_norm, like_pattern, like_pattern, source_uid_norm),
            ).fetchall()
            for candidate in rows:
                if build_item_variant_signature(dict(candidate)) == target_variant_sig:
                    return candidate
            return None

        source_uid = compact_spaces(str(data.get("source_uid") or ""))
        if not source_uid:
            return None
        rows = self.conn.execute(
            """
            SELECT * FROM items
            WHERE source_uid = ?
            ORDER BY created_at DESC
            """,
            (source_uid,),
        ).fetchall()
        for candidate in rows:
            if build_item_variant_signature(dict(candidate)) == target_variant_sig:
                return candidate
        return None

    def save_item(self, item: Dict[str, Any]) -> Tuple[int, bool, bool]:
        if not item.get("variant_signature"):
            try:
                item["variant_signature"] = build_item_variant_signature(item)
            except Exception:
                item["variant_signature"] = ""

        if not item.get("variant_components"):
            try:
                item["variant_components"] = get_item_variant_components(item)
            except Exception:
                item["variant_components"] = {}

        if not item.get("version_signature"):
            try:
                item["version_signature"] = build_version_signature(
                    source_uid=item.get("source_uid"),
                    media_type=item.get("media_type"),
                    source_title=item.get("source_title"),
                    source_episode_progress=item.get("source_episode_progress"),
                    source_format=item.get("source_format"),
                    source_audio_tracks=item.get("source_audio_tracks"),
                )
            except Exception:
                item["version_signature"] = ""

        data = {
            "source_uid": item["source_uid"],
            "version_signature": item["version_signature"],
            "source_title": item["source_title"],
            "source_link": item.get("source_link"),
            "kinozal_id": resolve_item_kinozal_id(item),
            "source_published_at": item.get("source_published_at"),
            "source_year": item.get("source_year"),
            "source_format": item.get("source_format"),
            "source_description": item.get("source_description"),
            "source_episode_progress": item.get("source_episode_progress"),
            "source_audio_tracks": json.dumps(item.get("source_audio_tracks", []), ensure_ascii=False),
            "imdb_id": item.get("imdb_id"),
            "mal_id": item.get("mal_id"),
            "cleaned_title": item.get("cleaned_title"),
            "source_category_id": item.get("source_category_id"),
            "source_category_name": item.get("source_category_name"),
            "media_type": item.get("media_type"),
            "tmdb_id": item.get("tmdb_id"),
            "tmdb_title": item.get("tmdb_title"),
            "tmdb_original_title": item.get("tmdb_original_title"),
            "tmdb_original_language": item.get("tmdb_original_language"),
            "tmdb_rating": item.get("tmdb_rating"),
            "tmdb_vote_count": item.get("tmdb_vote_count"),
            "tmdb_release_date": item.get("tmdb_release_date"),
            "tmdb_overview": item.get("tmdb_overview"),
            "tmdb_poster_url": item.get("tmdb_poster_url"),
            "tmdb_status": item.get("tmdb_status"),
            "tmdb_age_rating": item.get("tmdb_age_rating"),
            "tmdb_countries": json.dumps(parse_country_codes(item.get("tmdb_countries", [])), ensure_ascii=False),
            "tmdb_match_path": item.get("tmdb_match_path"),
            "tmdb_match_confidence": item.get("tmdb_match_confidence") or "",
            "tmdb_match_evidence": item.get("tmdb_match_evidence") or "",
            "tmdb_number_of_seasons": item.get("tmdb_number_of_seasons"),
            "tmdb_number_of_episodes": item.get("tmdb_number_of_episodes"),
            "tmdb_next_episode_name": item.get("tmdb_next_episode_name"),
            "tmdb_next_episode_air_date": item.get("tmdb_next_episode_air_date"),
            "tmdb_next_episode_season_number": item.get("tmdb_next_episode_season_number"),
            "tmdb_next_episode_episode_number": item.get("tmdb_next_episode_episode_number"),
            "tmdb_last_episode_name": item.get("tmdb_last_episode_name"),
            "tmdb_last_episode_air_date": item.get("tmdb_last_episode_air_date"),
            "tmdb_last_episode_season_number": item.get("tmdb_last_episode_season_number"),
            "tmdb_last_episode_episode_number": item.get("tmdb_last_episode_episode_number"),
            "manual_bucket": item.get("manual_bucket") or "",
            "manual_country_codes": ",".join(parse_country_codes(item.get("manual_country_codes"))) if item.get("manual_country_codes") is not None else "",
            "source_release_text": item.get("source_release_text") or "",
            "raw_json": json.dumps(item.get("raw_json", {}), ensure_ascii=False, sort_keys=True),
            "created_at": utc_ts(),
        }

        def pick_value(new_value: Any, old_value: Any) -> Any:
            if new_value is None:
                return old_value
            if isinstance(new_value, str):
                return new_value if compact_spaces(new_value) else old_value
            if isinstance(new_value, (list, tuple, set, dict)):
                return new_value if new_value else old_value
            return new_value

        with self.lock:
            existing = self._find_existing_item_for_upsert(data)
            if existing:
                existing_id = int(existing["id"])
                existing_data = dict(existing)
                merged = dict(data)
                for key in data:
                    if key == "created_at":
                        merged[key] = existing_data.get(key, data[key])
                        continue
                    merged[key] = pick_value(data.get(key), existing_data.get(key))

                fields_to_update = [
                    "source_uid",
                    "version_signature",
                    "source_title",
                    "source_link",
                    "kinozal_id",
                    "source_published_at",
                    "source_year",
                    "source_format",
                    "source_description",
                    "source_episode_progress",
                    "source_audio_tracks",
                    "imdb_id",
                    "mal_id",
                    "cleaned_title",
                    "source_category_id",
                    "source_category_name",
                    "media_type",
                    "tmdb_id",
                    "tmdb_title",
                    "tmdb_original_title",
                    "tmdb_original_language",
                    "tmdb_rating",
                    "tmdb_vote_count",
                    "tmdb_release_date",
                    "tmdb_overview",
                    "tmdb_poster_url",
                    "tmdb_status",
                    "tmdb_age_rating",
                    "tmdb_countries",
                    "tmdb_match_path",
                    "tmdb_match_confidence",
                    "tmdb_match_evidence",
                    "tmdb_number_of_seasons",
                    "tmdb_number_of_episodes",
                    "tmdb_next_episode_name",
                    "tmdb_next_episode_air_date",
                    "tmdb_next_episode_season_number",
                    "tmdb_next_episode_episode_number",
                    "tmdb_last_episode_name",
                    "tmdb_last_episode_air_date",
                    "tmdb_last_episode_season_number",
                    "tmdb_last_episode_episode_number",
                    "manual_bucket",
                    "manual_country_codes",
                    "source_release_text",
                    "raw_json",
                ]
                values = [merged[field] for field in fields_to_update]
                values.append(existing_id)

                current_genres = [
                    int(row["genre_id"])
                    for row in self.conn.execute(
                        "SELECT genre_id FROM item_genres WHERE item_id = ? ORDER BY genre_id",
                        (existing_id,),
                    ).fetchall()
                ]
                incoming_genres = sorted({int(value) for value in item.get("genre_ids", [])})
                final_genres = incoming_genres or current_genres

                materially_changed = False
                for field in fields_to_update:
                    if field in {"source_uid", "version_signature"}:
                        continue
                    if existing_data.get(field) != merged.get(field):
                        materially_changed = True
                        break
                if current_genres != final_genres:
                    materially_changed = True

                self.conn.execute(
                    f"UPDATE items SET {', '.join(f'{field} = ?' for field in fields_to_update)} WHERE id = ?",
                    values,
                )

                self.conn.execute("DELETE FROM item_genres WHERE item_id = ?", (existing_id,))
                if final_genres:
                    self.conn.executemany(
                        "INSERT INTO item_genres(item_id, genre_id) VALUES(?, ?)",
                        [(existing_id, genre_id) for genre_id in final_genres],
                    )
                self.conn.commit()
                return existing_id, False, materially_changed

            fields = ", ".join(data.keys())
            marks = ", ".join("?" for _ in data)
            row = self.conn.execute(
                f"INSERT INTO items({fields}) VALUES({marks}) RETURNING id",
                list(data.values()),
            ).fetchone()
            item_id = int(row["id"])

            genre_ids = sorted({int(value) for value in item.get("genre_ids", [])})
            if genre_ids:
                self.conn.executemany(
                    "INSERT INTO item_genres(item_id, genre_id) VALUES(?, ?)",
                    [(item_id, genre_id) for genre_id in genre_ids],
                )
            self.conn.commit()
            return item_id, True, True

    def get_item(self, item_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM items WHERE id = ?",
                (item_id,),
            ).fetchone()
            if not row:
                return None
            data = dict(row)
            genres = self.conn.execute(
                "SELECT genre_id FROM item_genres WHERE item_id = ? ORDER BY genre_id",
                (item_id,),
            ).fetchall()
            data["genre_ids"] = [int(value["genre_id"]) for value in genres]
            data["tmdb_countries"] = parse_country_codes(data.get("tmdb_countries"))
            data["manual_country_codes"] = parse_country_codes(data.get("manual_country_codes"))
            if not compact_spaces(str(data.get("kinozal_id") or "")):
                data["kinozal_id"] = resolve_item_kinozal_id(data)
            return data

    def clear_item_match(self, item_id: int) -> None:
        with self.lock:
            self.conn.execute(
                """
                UPDATE items
                SET imdb_id = NULL,
                    mal_id = NULL,
                    media_type = NULL,
                    tmdb_id = NULL,
                    tmdb_title = NULL,
                    tmdb_original_title = NULL,
                    tmdb_original_language = NULL,
                    tmdb_rating = NULL,
                    tmdb_vote_count = NULL,
                    tmdb_release_date = NULL,
                    tmdb_overview = NULL,
                    tmdb_poster_url = NULL,
                    tmdb_status = NULL,
                    tmdb_age_rating = NULL,
                    tmdb_countries = NULL,
                    tmdb_match_path = NULL,
                    tmdb_match_confidence = '',
                    tmdb_match_evidence = '',
                    tmdb_number_of_seasons = NULL,
                    tmdb_number_of_episodes = NULL,
                    tmdb_next_episode_name = NULL,
                    tmdb_next_episode_air_date = NULL,
                    tmdb_next_episode_season_number = NULL,
                    tmdb_next_episode_episode_number = NULL,
                    tmdb_last_episode_name = NULL,
                    tmdb_last_episode_air_date = NULL,
                    tmdb_last_episode_season_number = NULL,
                    tmdb_last_episode_episode_number = NULL
                WHERE id = ?
                """,
                (int(item_id),),
            )
            self.conn.execute("DELETE FROM item_genres WHERE item_id = ?", (int(item_id),))
            self.conn.commit()

    def find_item_by_kinozal_id(self, kinozal_id: str) -> Optional[Dict[str, Any]]:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return None
        with self.lock:
            row = self.conn.execute(
                """
                SELECT id FROM items
                WHERE kinozal_id = ? OR source_uid = ? OR source_uid LIKE ? OR source_link LIKE ?
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                LIMIT 1
                """,
                (kinozal_id, f"kinozal:{kinozal_id}", f"%details.php?id={kinozal_id}%", f"%id={kinozal_id}%"),
            ).fetchone()
            return self.db.get_item(int(row["id"])) if row else None

    def list_items_for_rematch(self, limit: int = 50, only_unmatched: bool = True) -> List[Dict[str, Any]]:
        fetch_limit = max(1, min(int(limit or 50), 500))
        where_clause = "WHERE COALESCE(kinozal_id, '') <> ''"
        if only_unmatched:
            where_clause += " AND tmdb_id IS NULL"
        with self.lock:
            rows = self.conn.execute(
                f"""
                SELECT id FROM items
                {where_clause}
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                LIMIT ?
                """,
                (fetch_limit,),
            ).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            item = self.db.get_item(int(row["id"]))
            if item:
                result.append(item)
        return result

    def set_item_manual_routing(
        self,
        item_id: int,
        bucket: str = "",
        country_codes: Optional[List[str]] = None,
    ) -> None:
        bucket_norm = str(bucket or "").strip().lower()
        if bucket_norm not in {"", "anime", "dorama", "regular"}:
            bucket_norm = ""
        countries_norm = ",".join(parse_country_codes(country_codes or []))
        with self.lock:
            self.conn.execute(
                "UPDATE items SET manual_bucket = ?, manual_country_codes = ? WHERE id = ?",
                (bucket_norm, countries_norm, int(item_id)),
            )
            self.conn.commit()

    def get_last_items(self, limit: int = 10) -> List[Dict[str, Any]]:
        fetch_limit = max(limit * 50, 200)
        with self.lock:
            rows = self.conn.execute(
                "SELECT id FROM items ORDER BY COALESCE(source_published_at, 0) DESC, id DESC LIMIT ?",
                (fetch_limit,),
            ).fetchall()
        result: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            item = self.db.get_item(int(row["id"]))
            if not item:
                continue
            kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
            key = kinozal_id or compact_spaces(str(item.get("source_uid") or "")) or f"item:{int(item.get('id') or 0)}"
            if key in seen:
                continue
            seen.add(key)
            result.append(item)
            if len(result) >= limit:
                break
        return result

    def get_last_items_for_subscription(self, sub_id: int, limit: int = 5) -> List[Dict[str, Any]]:
        items = self.db.get_last_items(limit=max(100, limit * 30))
        sub = self.db.get_subscription(sub_id)
        if not sub:
            return []
        matched = [item for item in items if item and match_subscription(self.db, sub, item)]
        matched.sort(
            key=lambda item: (int(item.get("source_published_at") or 0), int(item.get("id") or 0)),
            reverse=True,
        )
        return matched[:limit]

    def _archive_item_locked(
        self,
        item: Dict[str, Any],
        reason: str,
        merged_into_item_id: Optional[int] = None,
    ) -> bool:
        item_id = int(item["id"])
        existing = self.conn.execute("SELECT 1 FROM items WHERE id = ?", (item_id,)).fetchone()
        if not existing:
            return False
        full_item = self.db.get_item(item_id) or dict(item)
        kinozal_id = compact_spaces(str(full_item.get("kinozal_id") or "")) or resolve_item_kinozal_id(full_item)
        genre_ids = sorted({int(value) for value in full_item.get("genre_ids", [])})
        archived_at = utc_ts()
        self.conn.execute(
            """
            INSERT INTO items_archive(
                original_item_id, kinozal_id, source_uid, version_signature, source_title, source_link, media_type,
                source_published_at, source_year, source_format, source_description, source_episode_progress,
                source_audio_tracks, imdb_id, cleaned_title, source_category_id, source_category_name, tmdb_id, tmdb_title, tmdb_original_title,
                tmdb_original_language, tmdb_rating, tmdb_vote_count, tmdb_release_date, tmdb_status, tmdb_countries,
                manual_bucket, manual_country_codes, genre_ids, item_json, original_created_at, archived_at,
                archive_reason, merged_into_item_id
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?)
            """,
            (
                item_id,
                kinozal_id,
                full_item.get("source_uid"),
                full_item.get("version_signature"),
                full_item.get("source_title"),
                full_item.get("source_link"),
                full_item.get("media_type"),
                full_item.get("source_published_at"),
                full_item.get("source_year"),
                full_item.get("source_format"),
                full_item.get("source_description"),
                full_item.get("source_episode_progress"),
                json.dumps(full_item.get("source_audio_tracks", []), ensure_ascii=False),
                full_item.get("imdb_id"),
                full_item.get("cleaned_title"),
                full_item.get("source_category_id"),
                full_item.get("source_category_name"),
                full_item.get("tmdb_id"),
                full_item.get("tmdb_title"),
                full_item.get("tmdb_original_title"),
                full_item.get("tmdb_original_language"),
                full_item.get("tmdb_rating"),
                full_item.get("tmdb_vote_count"),
                full_item.get("tmdb_release_date"),
                full_item.get("tmdb_status"),
                json.dumps(parse_country_codes(full_item.get("tmdb_countries")), ensure_ascii=False),
                full_item.get("manual_bucket") or "",
                ",".join(parse_country_codes(full_item.get("manual_country_codes"))),
                json.dumps(genre_ids, ensure_ascii=False),
                json.dumps(full_item, ensure_ascii=False, sort_keys=True),
                full_item.get("created_at"),
                archived_at,
                compact_spaces(reason or "archive"),
                merged_into_item_id,
            ),
        )
        delivery_rows = self.conn.execute(
            "SELECT id, tg_user_id, item_id, subscription_id, matched_subscription_ids, delivery_audit_json, delivered_at FROM deliveries WHERE item_id = ? ORDER BY delivered_at ASC, id ASC",
            (item_id,),
        ).fetchall()
        for row in delivery_rows:
            delivery = dict(row)
            self.conn.execute(
                """
                INSERT INTO deliveries_archive(
                    original_delivery_id, tg_user_id, original_item_id, kinozal_id, source_uid, media_type, version_signature,
                    source_title, subscription_id, matched_subscription_ids, delivery_audit_json, delivered_at, archived_at, archive_reason, merged_into_item_id
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    delivery.get("id"),
                    delivery.get("tg_user_id"),
                    item_id,
                    kinozal_id,
                    full_item.get("source_uid"),
                    full_item.get("media_type"),
                    full_item.get("version_signature"),
                    full_item.get("source_title"),
                    delivery.get("subscription_id"),
                    delivery.get("matched_subscription_ids"),
                    delivery.get("delivery_audit_json") or "",
                    delivery.get("delivered_at"),
                    archived_at,
                    compact_spaces(reason or "archive"),
                    merged_into_item_id,
                ),
            )
        self.conn.execute("DELETE FROM items WHERE id = ?", (item_id,))
        return True

    def archive_item(self, item_id: int, reason: str, merged_into_item_id: Optional[int] = None) -> bool:
        with self.lock:
            item = self.db.get_item(int(item_id))
            if not item:
                return False
            archived = self._archive_item_locked(item, reason=reason, merged_into_item_id=merged_into_item_id)
            self.conn.commit()
            return archived

    def get_version_timeline(self, kinozal_id: str, limit: int = 10) -> Dict[str, Any]:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return {"kinozal_id": "", "active_count": 0, "archived_count": 0, "versions": []}
        with self.lock:
            active_rows = [
                dict(row)
                for row in self.conn.execute(
                    """
                    SELECT
                        id AS record_id,
                        kinozal_id,
                        source_uid,
                        version_signature,
                        source_title,
                        source_link,
                        media_type,
                        source_published_at,
                        source_year,
                        source_format,
                        source_episode_progress,
                        source_audio_tracks,
                        created_at,
                        NULL::BIGINT AS archived_at,
                        'active' AS state
                    FROM items
                    WHERE kinozal_id = ?
                    ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                    """,
                    (kinozal_id,),
                ).fetchall()
            ]
            archived_rows = [
                dict(row)
                for row in self.conn.execute(
                    """
                    SELECT
                        original_item_id AS record_id,
                        kinozal_id,
                        source_uid,
                        version_signature,
                        source_title,
                        source_link,
                        media_type,
                        source_published_at,
                        source_year,
                        source_format,
                        source_episode_progress,
                        source_audio_tracks,
                        original_created_at AS created_at,
                        archived_at,
                        'archived' AS state
                    FROM items_archive
                    WHERE kinozal_id = ?
                    ORDER BY COALESCE(source_published_at, 0) DESC, original_created_at DESC, archive_id DESC
                    """,
                    (kinozal_id,),
                ).fetchall()
            ]

        def sort_key(entry: Dict[str, Any]) -> tuple[int, int, int]:
            return (
                int(entry.get("source_published_at") or 0),
                int(entry.get("created_at") or 0),
                int(entry.get("record_id") or 0),
            )

        grouped: Dict[str, Dict[str, Any]] = {}
        for row in active_rows + archived_rows:
            row["kinozal_id"] = kinozal_id
            sig = compact_spaces(str(row.get("version_signature") or "")) or build_item_variant_signature(row)
            existing = grouped.get(sig)
            if not existing or sort_key(row) > sort_key(existing):
                row["version_duplicates"] = int(existing.get("version_duplicates") or 1) + 1 if existing else 1
                grouped[sig] = row
            elif existing:
                existing["version_duplicates"] = int(existing.get("version_duplicates") or 1) + 1

        versions = sorted(grouped.values(), key=sort_key, reverse=True)
        for entry in versions:
            entry["variant_summary"] = format_variant_summary(entry)
        return {
            "kinozal_id": kinozal_id,
            "active_count": len(active_rows),
            "archived_count": len(archived_rows),
            "versions": versions[: max(1, int(limit or 10))],
        }

    def cleanup_old_versions(
        self,
        keep_last: int = 3,
        dry_run: bool = True,
        preview_limit: int = 15,
    ) -> Dict[str, Any]:
        keep_last = max(1, int(keep_last or 1))
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT
                    id,
                    kinozal_id,
                    source_uid,
                    source_link,
                    source_title,
                    media_type,
                    version_signature,
                    source_published_at,
                    created_at,
                    source_format,
                    source_episode_progress,
                    source_audio_tracks
                FROM items
                WHERE COALESCE(kinozal_id, '') <> ''
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                """
            ).fetchall()
            by_release: Dict[Tuple[str, str], Dict[str, List[Dict[str, Any]]]] = {}
            for row in rows:
                item = dict(row)
                kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or resolve_item_kinozal_id(item)
                if not kinozal_id:
                    continue
                media_type = compact_spaces(str(item.get("media_type") or "movie")).lower() or "movie"
                version_sig = compact_spaces(str(item.get("version_signature") or "")) or build_item_variant_signature(item)
                by_release.setdefault((kinozal_id, media_type), {}).setdefault(version_sig, []).append(item)

            groups: List[Dict[str, Any]] = []
            versions_to_archive = 0
            items_to_archive = 0

            def item_sort_key(item: Dict[str, Any]) -> tuple[int, int, int]:
                return (
                    int(item.get("source_published_at") or 0),
                    int(item.get("created_at") or 0),
                    int(item.get("id") or 0),
                )

            for (kinozal_id, media_type), versions_map in by_release.items():
                version_groups: List[Dict[str, Any]] = []
                for version_sig, items in versions_map.items():
                    ordered_items = sorted(items, key=item_sort_key, reverse=True)
                    version_groups.append({
                        "version_signature": version_sig,
                        "items": ordered_items,
                        "representative": ordered_items[0],
                    })
                version_groups.sort(key=lambda group: item_sort_key(group["representative"]), reverse=True)
                if len(version_groups) <= keep_last:
                    continue
                keep_groups = version_groups[:keep_last]
                archive_groups = version_groups[keep_last:]
                archive_items = [item for group in archive_groups for item in group["items"]]
                groups.append({
                    "kinozal_id": kinozal_id,
                    "media_type": media_type,
                    "title": compact_spaces(
                        str(
                            (
                                keep_groups[0]["representative"].get("source_title")
                                if keep_groups
                                else archive_groups[0]["representative"].get("source_title")
                            )
                            or ""
                        )
                    ),
                    "keep_ids": [int(group["representative"]["id"]) for group in keep_groups],
                    "archive_ids": [int(item["id"]) for item in archive_items],
                    "versions_total": len(version_groups),
                    "versions_keep": len(keep_groups),
                    "versions_archive": len(archive_groups),
                })
                versions_to_archive += len(archive_groups)
                items_to_archive += len(archive_items)
                if not dry_run:
                    for archive_group in archive_groups:
                        for item in archive_group["items"]:
                            self._archive_item_locked(
                                item,
                                reason=f"keep_last_versions:{keep_last}",
                                merged_into_item_id=None,
                            )

            if not dry_run:
                self.conn.commit()

            groups.sort(key=lambda group: (group["versions_archive"], group["kinozal_id"]), reverse=True)
            summary = {
                "dry_run": dry_run,
                "groups": len(groups),
                "versions_to_archive": versions_to_archive,
                "items_to_archive": items_to_archive,
                "keep_last": keep_last,
                "sample_groups": groups[: max(1, int(preview_limit or 15))],
            }
            log.info(
                "Old versions cleanup dry_run=%s keep_last=%s groups=%s versions_to_archive=%s items_to_archive=%s",
                dry_run,
                keep_last,
                summary["groups"],
                summary["versions_to_archive"],
                summary["items_to_archive"],
            )
            return summary

    def cleanup_exact_duplicate_items(
        self,
        dry_run: bool = True,
        preview_limit: int = 15,
    ) -> Dict[str, Any]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT
                    id,
                    source_uid,
                    source_link,
                    source_title,
                    media_type,
                    source_published_at,
                    created_at,
                    source_format,
                    source_episode_progress,
                    source_audio_tracks,
                    imdb_id,
                    tmdb_id,
                    tmdb_overview,
                    tmdb_poster_url,
                    tmdb_status,
                    tmdb_vote_count,
                    tmdb_countries,
                    source_description,
                    manual_bucket,
                    kinozal_id
                FROM items
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                """
            ).fetchall()

            groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
            for row in rows:
                item = dict(row)
                kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
                if not kinozal_id:
                    continue
                media_type = compact_spaces(str(item.get("media_type") or "movie")).lower() or "movie"
                variant_sig = build_item_variant_signature(item)
                groups.setdefault((kinozal_id, media_type, variant_sig), []).append(item)

            duplicate_groups: List[Dict[str, Any]] = []
            items_to_delete = 0
            deliveries_to_migrate = 0

            for (kinozal_id, media_type, _variant_sig), items in groups.items():
                if len(items) < 2:
                    continue
                ordered = sorted(
                    items,
                    key=lambda item: (
                        item_duplicate_quality_score(item),
                        int(item.get("source_published_at") or 0),
                        int(item.get("created_at") or 0),
                        int(item.get("id") or 0),
                    ),
                    reverse=True,
                )
                keeper = ordered[0]
                losers = ordered[1:]
                moved_for_group = 0
                for loser in losers:
                    row = self.conn.execute(
                        "SELECT COUNT(*) AS cnt FROM deliveries WHERE item_id = ?",
                        (int(loser["id"]),),
                    ).fetchone()
                    moved_for_group += int((row or {}).get("cnt") or 0)

                duplicate_groups.append({
                    "kinozal_id": kinozal_id,
                    "media_type": media_type,
                    "title": compact_spaces(str(keeper.get("source_title") or losers[0].get("source_title") or "")),
                    "keep_id": int(keeper["id"]),
                    "remove_ids": [int(item["id"]) for item in losers],
                    "count": len(items),
                    "deliveries_to_migrate": moved_for_group,
                })
                items_to_delete += len(losers)
                deliveries_to_migrate += moved_for_group

                if not dry_run:
                    for loser in losers:
                        keeper_id = int(keeper["id"])
                        loser_id = int(loser["id"])
                        self.conn.execute(
                            """
                            INSERT INTO deliveries(tg_user_id, item_id, subscription_id, matched_subscription_ids, delivery_audit_json, delivered_at)
                            SELECT tg_user_id, ?, subscription_id, matched_subscription_ids, delivery_audit_json, delivered_at
                            FROM deliveries
                            WHERE item_id = ?
                            ON CONFLICT(tg_user_id, item_id) DO NOTHING
                            """,
                            (keeper_id, loser_id),
                        )
                        self._archive_item_locked(
                            loser,
                            reason="exact_duplicate_cleanup",
                            merged_into_item_id=keeper_id,
                        )

            duplicate_groups.sort(key=lambda group: (group["count"], group["kinozal_id"]), reverse=True)
            summary = {
                "dry_run": dry_run,
                "groups": len(duplicate_groups),
                "items_to_delete": items_to_delete,
                "deliveries_to_migrate": deliveries_to_migrate,
                "sample_groups": duplicate_groups[: max(1, int(preview_limit or 15))],
            }
            log.info(
                "Duplicate cleanup dry_run=%s groups=%s items_to_delete=%s deliveries_to_migrate=%s",
                dry_run,
                summary["groups"],
                summary["items_to_delete"],
                summary["deliveries_to_migrate"],
            )
            return summary

    def update_item_release_text(self, item_id: int, text: str) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE items SET source_release_text = ? WHERE id = ?",
                (text or "", item_id),
            )
            self.conn.commit()
