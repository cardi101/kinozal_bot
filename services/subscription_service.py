from typing import Any, Dict, List

from subscription_matching import match_subscription


class SubscriptionService:
    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def list_enabled(self) -> List[Dict[str, Any]]:
        enabled_subs = [
            self.repository.get_subscription(int(sub["id"]))
            for sub in self.repository.list_enabled_subscriptions()
        ]
        return [sub for sub in enabled_subs if sub]

    def matches(self, sub: Dict[str, Any], item: Dict[str, Any]) -> bool:
        return match_subscription(self.repository, sub, item)
