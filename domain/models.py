from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass(slots=True)
class ReleaseItem:
    payload: Dict[str, Any]

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "ReleaseItem":
        return cls(dict(payload))

    def to_dict(self) -> Dict[str, Any]:
        return self.payload

    def clone(self) -> "ReleaseItem":
        return ReleaseItem.from_payload(self.payload)

    def get(self, key: str, default: Any = None) -> Any:
        return self.payload.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.payload[key] = value

    @property
    def id(self) -> int:
        return int(self.payload.get("id") or 0)

    @property
    def tg_item_id(self) -> int:
        return self.id

    @property
    def tmdb_id(self) -> int | None:
        value = self.payload.get("tmdb_id")
        return int(value) if value not in (None, "") else None

    @property
    def kinozal_id(self) -> str:
        return str(self.payload.get("kinozal_id") or "")

    @property
    def source_uid(self) -> str:
        return str(self.payload.get("source_uid") or "")

    @property
    def source_title(self) -> str:
        return str(self.payload.get("source_title") or "")


@dataclass(slots=True)
class SubscriptionRecord:
    payload: Dict[str, Any]

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "SubscriptionRecord":
        return cls(dict(payload))

    def to_dict(self) -> Dict[str, Any]:
        return self.payload

    def get(self, key: str, default: Any = None) -> Any:
        return self.payload.get(key, default)

    @property
    def id(self) -> int:
        return int(self.payload["id"])

    @property
    def tg_user_id(self) -> int:
        return int(self.payload["tg_user_id"])


@dataclass(slots=True)
class CompiledSubscription:
    payload: Dict[str, Any]
    media_type: str
    year_from: Optional[int]
    year_to: Optional[int]
    allow_formats: Tuple[str, ...]
    min_rating: Optional[float]
    genre_ids: Tuple[int, ...]
    content_filter: str
    country_codes: Tuple[str, ...]
    exclude_country_codes: Tuple[str, ...]
    include_keywords: Tuple[str, ...]
    exclude_keywords: Tuple[str, ...]
    include_keyword_modes: Dict[str, str]
    exclude_keyword_modes: Dict[str, str]

    @classmethod
    def from_payload(
        cls,
        payload: Dict[str, Any],
        *,
        media_type: str,
        year_from: Optional[int],
        year_to: Optional[int],
        allow_formats: Tuple[str, ...],
        min_rating: Optional[float],
        genre_ids: Tuple[int, ...],
        content_filter: str,
        country_codes: Tuple[str, ...],
        exclude_country_codes: Tuple[str, ...],
        include_keywords: Tuple[str, ...],
        exclude_keywords: Tuple[str, ...],
        include_keyword_modes: Dict[str, str],
        exclude_keyword_modes: Dict[str, str],
    ) -> "CompiledSubscription":
        return cls(
            payload=dict(payload),
            media_type=media_type,
            year_from=year_from,
            year_to=year_to,
            allow_formats=allow_formats,
            min_rating=min_rating,
            genre_ids=genre_ids,
            content_filter=content_filter,
            country_codes=country_codes,
            exclude_country_codes=exclude_country_codes,
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
            include_keyword_modes=dict(include_keyword_modes),
            exclude_keyword_modes=dict(exclude_keyword_modes),
        )

    def to_dict(self) -> Dict[str, Any]:
        return self.payload

    def get(self, key: str, default: Any = None) -> Any:
        return self.payload.get(key, default)

    @property
    def id(self) -> int:
        return int(self.payload["id"])

    @property
    def tg_user_id(self) -> int:
        return int(self.payload["tg_user_id"])


@dataclass(slots=True)
class DeliveryCandidate:
    item: ReleaseItem
    subs: List[Any] = field(default_factory=list)
    old_release_text: str = ""
    is_release_text_change: bool = False
    debounce_kinozal_id: str = ""
    delivery_context: str = ""
    event_type: str = ""
    event_key: str = ""
    queue_lease_token: str = ""

    @property
    def item_id(self) -> int:
        return self.item.id

    def subs_payloads(self) -> List[Dict[str, Any]]:
        return [sub.to_dict() for sub in self.subs]
