CREATE TABLE IF NOT EXISTS delivery_claims (
    id BIGSERIAL PRIMARY KEY,
    tg_user_id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    kinozal_id TEXT,
    source_uid TEXT,
    version_signature TEXT,
    subscription_id BIGINT,
    matched_subscription_ids TEXT,
    delivery_context TEXT NOT NULL DEFAULT '',
    delivery_audit_json TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'sending',
    last_error TEXT NOT NULL DEFAULT '',
    claimed_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    sent_at BIGINT,
    UNIQUE(tg_user_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_delivery_claims_user_status ON delivery_claims(tg_user_id, status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_delivery_claims_user_kinozal ON delivery_claims(tg_user_id, kinozal_id, status, updated_at DESC);
