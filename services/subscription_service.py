from typing import Any, List

from domain import CompiledSubscription, ReleaseItem, SubscriptionRecord
from subscription_matching import (
    compile_subscription,
    explain_subscription_match,
    explain_subscription_match_details,
    match_subscription,
)


class SubscriptionService:
    def __init__(self, repository: Any) -> None:
        self.repository = repository

    def list_enabled(self) -> List[SubscriptionRecord]:
        return [SubscriptionRecord.from_payload(sub) for sub in self.repository.list_enabled_subscriptions() if sub]

    def list_enabled_compiled(self) -> List[CompiledSubscription]:
        return [
            compile_subscription(self.repository, sub)
            for sub in self.repository.list_enabled_subscriptions()
            if sub
        ]

    def compile(self, sub: SubscriptionRecord | dict | CompiledSubscription) -> CompiledSubscription:
        return compile_subscription(self.repository, sub)

    def matches(self, sub: SubscriptionRecord, item: ReleaseItem) -> bool:
        return match_subscription(self.repository, sub, item)

    def explain(self, sub: SubscriptionRecord | dict | CompiledSubscription, item: ReleaseItem) -> str:
        return explain_subscription_match(self.repository, sub, item)

    def explain_details(self, sub: SubscriptionRecord | dict | CompiledSubscription, item: ReleaseItem) -> dict:
        return explain_subscription_match_details(self.repository, sub, item)
