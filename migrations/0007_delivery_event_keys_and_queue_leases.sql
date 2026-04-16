ALTER TABLE users ADD COLUMN IF NOT EXISTS quiet_timezone TEXT NOT NULL DEFAULT '';

ALTER TABLE delivery_claims ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'release';
ALTER TABLE delivery_claims ADD COLUMN IF NOT EXISTS event_key TEXT NOT NULL DEFAULT '';

UPDATE delivery_claims
SET event_key = CONCAT(
        CASE
            WHEN COALESCE(event_type, '') = 'release_text' OR COALESCE(delivery_context, '') = 'release_text_update'
                THEN 'release_text'
            WHEN COALESCE(event_type, '') = 'grouped' OR COALESCE(delivery_context, '') LIKE 'grouped%'
                THEN 'grouped'
            ELSE 'release'
        END,
        ':',
        tg_user_id,
        ':',
        COALESCE(NULLIF(kinozal_id, ''), NULLIF(source_uid, ''), item_id::TEXT),
        ':',
        COALESCE(NULLIF(version_signature, ''), item_id::TEXT)
    )
WHERE COALESCE(event_key, '') = '';

ALTER TABLE delivery_claims DROP CONSTRAINT IF EXISTS delivery_claims_tg_user_id_item_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_claims_user_event_key
    ON delivery_claims(tg_user_id, event_key);

ALTER TABLE pending_deliveries ADD COLUMN IF NOT EXISTS event_type TEXT NOT NULL DEFAULT 'release';
ALTER TABLE pending_deliveries ADD COLUMN IF NOT EXISTS event_key TEXT NOT NULL DEFAULT '';
ALTER TABLE pending_deliveries ADD COLUMN IF NOT EXISTS deliver_not_before_ts BIGINT NOT NULL DEFAULT 0;
ALTER TABLE pending_deliveries ADD COLUMN IF NOT EXISTS lease_token TEXT NOT NULL DEFAULT '';
ALTER TABLE pending_deliveries ADD COLUMN IF NOT EXISTS lease_expires_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE pending_deliveries ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE pending_deliveries ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '';

UPDATE pending_deliveries
SET deliver_not_before_ts = CASE
        WHEN COALESCE(deliver_not_before_ts, 0) > 0 THEN deliver_not_before_ts
        ELSE queued_at
    END,
    event_type = CASE
        WHEN is_release_text_change = 1 THEN 'release_text'
        ELSE 'release'
    END,
    event_key = CASE
        WHEN COALESCE(event_key, '') <> '' THEN event_key
        WHEN is_release_text_change = 1 THEN CONCAT('release_text:', tg_user_id, ':', item_id, ':legacy')
        ELSE CONCAT('release:', tg_user_id, ':', item_id, ':legacy')
    END;

ALTER TABLE pending_deliveries DROP CONSTRAINT IF EXISTS pending_deliveries_tg_user_id_item_id_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_pending_deliveries_user_event_key
    ON pending_deliveries(tg_user_id, event_key);
CREATE INDEX IF NOT EXISTS idx_pending_deliveries_due_lease
    ON pending_deliveries(deliver_not_before_ts, lease_expires_at, queued_at);

ALTER TABLE debounce_queue ADD COLUMN IF NOT EXISTS event_key TEXT NOT NULL DEFAULT '';
ALTER TABLE debounce_queue ADD COLUMN IF NOT EXISTS lease_token TEXT NOT NULL DEFAULT '';
ALTER TABLE debounce_queue ADD COLUMN IF NOT EXISTS lease_expires_at BIGINT NOT NULL DEFAULT 0;
ALTER TABLE debounce_queue ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE debounce_queue ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '';

UPDATE debounce_queue
SET event_key = CASE
        WHEN COALESCE(event_key, '') <> '' THEN event_key
        ELSE CONCAT('debounce:', tg_user_id, ':', kinozal_id)
    END;

CREATE INDEX IF NOT EXISTS idx_debounce_queue_due_lease
    ON debounce_queue(deliver_after_ts, lease_expires_at);

CREATE TABLE IF NOT EXISTS telegram_file_cache (
    cache_key TEXT PRIMARY KEY,
    file_id TEXT NOT NULL,
    file_unique_id TEXT NOT NULL DEFAULT '',
    updated_at BIGINT NOT NULL
);
