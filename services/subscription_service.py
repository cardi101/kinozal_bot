from typing import Any, List

from domain import ReleaseItem, SubscriptionRecord
from subscription_matching import match_subscription


class SubscriptionService:
    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def list_enabled(self) -> List[SubscriptionRecord]:
        enabled_subs = [
            self.repository.get_subscription(int(sub["id"]))
            for sub in self.repository.list_enabled_subscriptions()
        ]
        return [SubscriptionRecord.from_payload(sub) for sub in enabled_subs if sub]

    def matches(self, sub: SubscriptionRecord, item: ReleaseItem) -> bool:
        return match_subscription(self.repository, sub.to_dict(), item.to_dict())
