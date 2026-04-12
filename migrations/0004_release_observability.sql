CREATE TABLE IF NOT EXISTS source_observations (
    id BIGSERIAL PRIMARY KEY,
    kinozal_id TEXT NOT NULL,
    item_id BIGINT,
    poll_ts BIGINT NOT NULL,
    source_kind TEXT NOT NULL,
    source_title TEXT NOT NULL DEFAULT '',
    details_title TEXT NOT NULL DEFAULT '',
    episode_progress TEXT NOT NULL DEFAULT '',
    release_text_hash TEXT NOT NULL DEFAULT '',
    source_format TEXT NOT NULL DEFAULT '',
    audio_sig TEXT NOT NULL DEFAULT '',
    raw_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at BIGINT NOT NULL,
    CONSTRAINT fk_source_observations_item FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_source_observations_kinozal_poll
    ON source_observations(kinozal_id, poll_ts DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_source_observations_item
    ON source_observations(item_id, poll_ts DESC, id DESC);

CREATE TABLE IF NOT EXISTS release_anomalies (
    id BIGSERIAL PRIMARY KEY,
    kinozal_id TEXT NOT NULL,
    item_id BIGINT,
    anomaly_type TEXT NOT NULL,
    old_value TEXT NOT NULL DEFAULT '',
    new_value TEXT NOT NULL DEFAULT '',
    details TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CONSTRAINT fk_release_anomalies_item FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_release_anomalies_kinozal_created
    ON release_anomalies(kinozal_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_release_anomalies_status_created
    ON release_anomalies(status, created_at DESC, id DESC);
