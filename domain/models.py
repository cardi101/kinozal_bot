from dataclasses import dataclass, field
from typing import Any, Dict, List


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
class DeliveryCandidate:
    item: ReleaseItem
    subs: List[SubscriptionRecord] = field(default_factory=list)
    old_release_text: str = ""
    is_release_text_change: bool = False

    @property
    def item_id(self) -> int:
        return self.item.id

    def subs_payloads(self) -> List[Dict[str, Any]]:
        return [sub.to_dict() for sub in self.subs]
