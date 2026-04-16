from typing import Dict, List, Optional

from country_helpers import COUNTRY_NAMES_RU, country_name_ru, parse_country_codes
from utils import utc_ts

from .base import BaseRepository

try:
    import pycountry
except Exception:
    pycountry = None


class MetaRepository(BaseRepository):
    def get_telegram_file_cache(self, cache_key: str) -> Optional[str]:
        with self.lock:
            row = self.conn.execute(
                "SELECT file_id FROM telegram_file_cache WHERE cache_key = ?",
                (str(cache_key or ""),),
            ).fetchone()
            return str(row["file_id"]) if row and row.get("file_id") else None

    def set_telegram_file_cache(self, cache_key: str, file_id: str, file_unique_id: str = "") -> None:
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO telegram_file_cache(cache_key, file_id, file_unique_id, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    file_id = excluded.file_id,
                    file_unique_id = excluded.file_unique_id,
                    updated_at = excluded.updated_at
                """,
                (str(cache_key or ""), str(file_id or ""), str(file_unique_id or ""), ts),
            )
            self.conn.commit()

    def upsert_genres(self, media_type: str, genre_map: Dict[int, str]) -> None:
        ts = utc_ts()
        with self.lock:
            for genre_id, name in genre_map.items():
                self.conn.execute(
                    """
                    INSERT INTO genres(media_type, genre_id, name, updated_at)
                    VALUES(?, ?, ?, ?)
                    ON CONFLICT(media_type, genre_id) DO UPDATE SET
                        name = excluded.name,
                        updated_at = excluded.updated_at
                    """,
                    (media_type, int(genre_id), name, ts),
                )
            self.conn.commit()

    def get_genres(self, media_type: str) -> Dict[int, str]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT genre_id, name FROM genres WHERE media_type = ? ORDER BY name ASC",
                (media_type,),
            ).fetchall()
            return {int(row["genre_id"]): row["name"] for row in rows}

    def get_all_genres_merged(self) -> Dict[int, str]:
        result: Dict[int, str] = {}
        for media_type in ("movie", "tv"):
            result.update(self.db.get_genres(media_type))
        return dict(sorted(result.items(), key=lambda item: item[1].lower()))

    def get_meta(self, key: str) -> Optional[str]:
        with self.lock:
            row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO meta(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, ts),
            )
            self.conn.commit()

    def get_known_country_codes(self) -> List[str]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT tmdb_countries FROM items WHERE tmdb_countries IS NOT NULL AND tmdb_countries <> ''"
            ).fetchall()

        codes = set(COUNTRY_NAMES_RU.keys())
        for row in rows:
            codes.update(parse_country_codes(row["tmdb_countries"]))

        if pycountry is not None:
            try:
                codes.update(
                    str(country.alpha_2).upper()
                    for country in pycountry.countries
                    if getattr(country, "alpha_2", None)
                )
            except Exception:
                pass

        return sorted(codes, key=lambda code: country_name_ru(code).lower())
