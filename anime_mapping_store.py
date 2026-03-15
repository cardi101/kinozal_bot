import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional


def _compact(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _normalize_title(text: str) -> str:
    text = _compact(text).casefold()
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\\/|]+", " ", text)
    text = re.sub(r"[^\w\s]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _first_intish(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    raw = raw.split(",")[0].strip()
    m = re.search(r"\d+", raw)
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None


@dataclass
class AnimeMappingEntry:
    tmdb_id: int
    media_type: str
    source: str
    titles: List[str]
    year: Optional[int] = None
    raw: Optional[Dict[str, Any]] = None


class AnimeMappingStore:
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.loaded = False
        self.entries: List[AnimeMappingEntry] = []
        self.by_title: Dict[str, List[AnimeMappingEntry]] = {}
        self.by_tmdb_id: Dict[int, AnimeMappingEntry] = {}
        self.kometa_ids: Dict[str, Dict[str, Any]] = {}

    def _iter_files(self) -> Iterable[Path]:
        if not self.base_dir.exists():
            return []
        return sorted(self.base_dir.rglob("*.json"))

    def _load_json(self, path: Path) -> Any:
        return json.loads(path.read_text(encoding="utf-8"))

    def _load_kometa_ids(self) -> None:
        self.kometa_ids = {}
        path = self.base_dir / "kometa" / "anime_ids.json"
        if not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(payload, dict):
            return
        for key, value in payload.items():
            if isinstance(value, dict):
                self.kometa_ids[str(key)] = value

    def _iter_records(self, payload: Any, path: Path) -> Iterable[Dict[str, Any]]:
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    yield item
            return

        if not isinstance(payload, dict):
            return

        if path.name.startswith("anime-offline-database") and isinstance(payload.get("data"), list):
            for item in payload["data"]:
                if isinstance(item, dict):
                    yield item
            return

        for key in ("data", "items", "anime", "entries", "mappings", "shows"):
            value = payload.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        yield item
                return

        yield payload

    def _extract_titles(self, record: Dict[str, Any]) -> List[str]:
        result: List[str] = []

        def push(value: Any) -> None:
            if isinstance(value, str):
                value = _compact(value)
                if value and value not in result:
                    result.append(value)
            elif isinstance(value, list):
                for item in value:
                    push(item)
            elif isinstance(value, dict):
                for item in value.values():
                    push(item)

        for key in ("title", "titles", "name", "names", "aliases", "synonyms"):
            push(record.get(key))

        for key in ("tmdb_title", "anilist_title", "mal_title", "original_title"):
            push(record.get(key))

        return result

    def _extract_year(self, record: Dict[str, Any]) -> Optional[int]:
        for key in ("year", "season_year", "release_year", "start_year"):
            value = record.get(key)
            try:
                if value is not None and str(value).strip():
                    return int(str(value).strip()[:4])
            except Exception:
                continue

        anime_season = record.get("animeSeason")
        if isinstance(anime_season, dict):
            value = anime_season.get("year")
            try:
                if value is not None and str(value).strip():
                    return int(str(value).strip()[:4])
            except Exception:
                pass

        return None

    def _extract_anidb_id(self, record: Dict[str, Any]) -> Optional[str]:
        sources = record.get("sources") or []
        if not isinstance(sources, list):
            return None

        patterns = [
            r"anidb\.net/anime/(\d+)",
            r"anidb\.net/a(\d+)",
        ]

        for src in sources:
            if not isinstance(src, str):
                continue
            for pattern in patterns:
                m = re.search(pattern, src, flags=re.I)
                if m:
                    try:
                        return str(int(m.group(1)))
                    except Exception:
                        return None
        return None

    def _kometa_tmdb_from_anidb(self, anidb_id: str) -> Optional[Dict[str, Any]]:
        ids = self.kometa_ids.get(str(anidb_id))
        if not ids:
            return None

        show_id = _first_intish(ids.get("tmdb_show_id"))
        movie_id = _first_intish(ids.get("tmdb_movie_id"))

        if show_id is not None:
            return {"tmdb_id": show_id, "media_type": "tv"}
        if movie_id is not None:
            return {"tmdb_id": movie_id, "media_type": "movie"}
        return None

    def _extract_tmdb(self, record: Dict[str, Any], source_name: str, path: Path) -> Optional[AnimeMappingEntry]:
        tmdb_movie_id = _first_intish(record.get("tmdb_movie_id"))
        tmdb_show_id = _first_intish(record.get("tmdb_show_id"))
        tmdb_id = _first_intish(record.get("tmdb_id") or record.get("themoviedb_id"))

        media_type = str(record.get("media_type") or "").strip().lower()

        if tmdb_show_id is not None:
            return AnimeMappingEntry(
                tmdb_id=tmdb_show_id,
                media_type="tv",
                source=source_name,
                titles=self._extract_titles(record),
                year=self._extract_year(record),
                raw=record,
            )

        if tmdb_movie_id is not None:
            return AnimeMappingEntry(
                tmdb_id=tmdb_movie_id,
                media_type="movie",
                source=source_name,
                titles=self._extract_titles(record),
                year=self._extract_year(record),
                raw=record,
            )

        if tmdb_id is not None:
            guessed_media = media_type if media_type in {"tv", "movie"} else "tv"
            return AnimeMappingEntry(
                tmdb_id=tmdb_id,
                media_type=guessed_media,
                source=source_name,
                titles=self._extract_titles(record),
                year=self._extract_year(record),
                raw=record,
            )

        if path.name.startswith("anime-offline-database"):
            anidb_id = self._extract_anidb_id(record)
            if not anidb_id:
                return None

            tmdb = self._kometa_tmdb_from_anidb(anidb_id)
            if not tmdb:
                return None

            return AnimeMappingEntry(
                tmdb_id=int(tmdb["tmdb_id"]),
                media_type=str(tmdb["media_type"]),
                source=source_name,
                titles=self._extract_titles(record),
                year=self._extract_year(record),
                raw=record,
            )

        return None

    def load(self, force: bool = False) -> None:
        if self.loaded and not force:
            return

        self.entries = []
        self.by_title = {}
        self.by_tmdb_id = {}
        self._load_kometa_ids()

        for path in self._iter_files():
            if path.name == "anime_ids.json" and path.parent.name == "kometa":
                continue

            try:
                payload = self._load_json(path)
            except Exception:
                continue

            source_name = path.parent.name or path.stem

            for record in self._iter_records(payload, path):
                entry = self._extract_tmdb(record, source_name, path)
                if not entry:
                    continue

                self.entries.append(entry)
                self.by_tmdb_id[entry.tmdb_id] = entry

                for title in entry.titles:
                    norm = _normalize_title(title)
                    if not norm:
                        continue
                    self.by_title.setdefault(norm, []).append(entry)

        self.loaded = True

    def find_by_normalized_title(self, title_norm: str) -> List[AnimeMappingEntry]:
        if not self.loaded:
            self.load()
        norm = _normalize_title(title_norm)
        return list(self.by_title.get(norm, []))

    def find_best(self, title_norms: List[str], year: Optional[int] = None) -> Optional[AnimeMappingEntry]:
        if not self.loaded:
            self.load()

        normalized_queries = []
        for norm in title_norms:
            nn = _normalize_title(norm)
            if nn and nn not in normalized_queries:
                normalized_queries.append(nn)

        best: Optional[AnimeMappingEntry] = None
        best_score = -1

        seen_ids = set()
        candidates: List[AnimeMappingEntry] = []

        for norm in normalized_queries:
            for entry in self.by_title.get(norm, []):
                key = (entry.tmdb_id, entry.source)
                if key in seen_ids:
                    continue
                seen_ids.add(key)
                candidates.append(entry)

        for entry in candidates:
            score = 0

            normalized_titles = {_normalize_title(title) for title in entry.titles if _normalize_title(title)}
            if any(norm in normalized_titles for norm in normalized_queries):
                score += 100

            if year is not None and entry.year is not None:
                delta = abs(entry.year - year)
                if delta == 0:
                    score += 25
                elif delta == 1:
                    score += 15
                elif delta == 2:
                    score += 5

            if entry.media_type == "tv":
                score += 3

            if entry.source == "manual":
                score += 1000

            if score > best_score:
                best = entry
                best_score = score

        return best

