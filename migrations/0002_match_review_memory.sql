ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_match_path TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_match_confidence TEXT NOT NULL DEFAULT '';
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_match_evidence TEXT NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS match_review_queue (
    item_id BIGINT PRIMARY KEY,
    kinozal_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    reason TEXT NOT NULL DEFAULT '',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    notified_at BIGINT,
    decided_at BIGINT,
    decision_by BIGINT,
    decision_note TEXT NOT NULL DEFAULT '',
    CONSTRAINT fk_match_review_item FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_match_review_queue_status_created
    ON match_review_queue(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_match_review_queue_kinozal
    ON match_review_queue(kinozal_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS match_overrides (
    kinozal_id TEXT PRIMARY KEY,
    tmdb_id INTEGER NOT NULL,
    media_type TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'admin',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS match_rejections (
    kinozal_id TEXT NOT NULL,
    tmdb_id INTEGER NOT NULL,
    created_at BIGINT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (kinozal_id, tmdb_id)
);
