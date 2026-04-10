CREATE TABLE IF NOT EXISTS users (
    tg_user_id BIGINT PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    access_granted INTEGER NOT NULL DEFAULT 0,
    access_expires_at BIGINT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS invites (
    code TEXT PRIMARY KEY,
    uses_left INTEGER NOT NULL,
    expires_at BIGINT,
    note TEXT,
    created_by BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id BIGSERIAL PRIMARY KEY,
    tg_user_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    media_type TEXT NOT NULL DEFAULT 'any',
    year_from INTEGER,
    year_to INTEGER,
    allow_720 INTEGER NOT NULL DEFAULT 0,
    allow_1080 INTEGER NOT NULL DEFAULT 0,
    allow_2160 INTEGER NOT NULL DEFAULT 0,
    min_tmdb_rating DOUBLE PRECISION,
    include_keywords TEXT NOT NULL DEFAULT '',
    exclude_keywords TEXT NOT NULL DEFAULT '',
    content_filter TEXT NOT NULL DEFAULT 'any',
    country_codes TEXT NOT NULL DEFAULT '',
    exclude_country_codes TEXT NOT NULL DEFAULT '',
    preset_key TEXT NOT NULL DEFAULT '',
    is_enabled INTEGER NOT NULL DEFAULT 1,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CONSTRAINT fk_subscriptions_user FOREIGN KEY(tg_user_id) REFERENCES users(tg_user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS subscription_genres (
    subscription_id BIGINT NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (subscription_id, genre_id),
    CONSTRAINT fk_subscription_genres_sub FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS genres (
    media_type TEXT NOT NULL,
    genre_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    updated_at BIGINT NOT NULL,
    PRIMARY KEY (media_type, genre_id)
);

CREATE TABLE IF NOT EXISTS items (
    id BIGSERIAL PRIMARY KEY,
    source_uid TEXT NOT NULL,
    version_signature TEXT NOT NULL,
    source_title TEXT NOT NULL,
    source_link TEXT,
    kinozal_id TEXT,
    source_published_at BIGINT,
    source_year INTEGER,
    source_format TEXT,
    source_description TEXT,
    source_episode_progress TEXT,
    source_audio_tracks TEXT,
    imdb_id TEXT,
    mal_id TEXT,
    cleaned_title TEXT,
    source_category_id TEXT,
    source_category_name TEXT,
    media_type TEXT,
    tmdb_id INTEGER,
    tmdb_title TEXT,
    tmdb_original_title TEXT,
    tmdb_original_language TEXT,
    tmdb_rating DOUBLE PRECISION,
    tmdb_vote_count INTEGER,
    tmdb_release_date TEXT,
    tmdb_overview TEXT,
    tmdb_poster_url TEXT,
    tmdb_status TEXT,
    tmdb_age_rating TEXT,
    tmdb_countries TEXT,
    tmdb_number_of_seasons INTEGER,
    tmdb_number_of_episodes INTEGER,
    tmdb_next_episode_name TEXT,
    tmdb_next_episode_air_date TEXT,
    tmdb_next_episode_season_number INTEGER,
    tmdb_next_episode_episode_number INTEGER,
    tmdb_last_episode_name TEXT,
    tmdb_last_episode_air_date TEXT,
    tmdb_last_episode_season_number INTEGER,
    tmdb_last_episode_episode_number INTEGER,
    manual_bucket TEXT NOT NULL DEFAULT '',
    manual_country_codes TEXT NOT NULL DEFAULT '',
    raw_json JSONB NOT NULL,
    created_at BIGINT NOT NULL,
    UNIQUE(source_uid, version_signature)
);

CREATE TABLE IF NOT EXISTS items_archive (
    archive_id BIGSERIAL PRIMARY KEY,
    original_item_id BIGINT NOT NULL,
    kinozal_id TEXT,
    source_uid TEXT NOT NULL,
    version_signature TEXT NOT NULL,
    source_title TEXT NOT NULL,
    source_link TEXT,
    media_type TEXT,
    source_published_at BIGINT,
    source_year INTEGER,
    source_format TEXT,
    source_description TEXT,
    source_episode_progress TEXT,
    source_audio_tracks TEXT,
    imdb_id TEXT,
    cleaned_title TEXT,
    source_category_id TEXT,
    source_category_name TEXT,
    tmdb_id INTEGER,
    tmdb_title TEXT,
    tmdb_original_title TEXT,
    tmdb_original_language TEXT,
    tmdb_rating DOUBLE PRECISION,
    tmdb_vote_count INTEGER,
    tmdb_release_date TEXT,
    tmdb_status TEXT,
    tmdb_countries TEXT,
    manual_bucket TEXT NOT NULL DEFAULT '',
    manual_country_codes TEXT NOT NULL DEFAULT '',
    genre_ids TEXT,
    item_json JSONB NOT NULL,
    original_created_at BIGINT,
    archived_at BIGINT NOT NULL,
    archive_reason TEXT NOT NULL,
    merged_into_item_id BIGINT
);

CREATE TABLE IF NOT EXISTS deliveries_archive (
    archive_id BIGSERIAL PRIMARY KEY,
    original_delivery_id BIGINT,
    tg_user_id BIGINT NOT NULL,
    original_item_id BIGINT NOT NULL,
    kinozal_id TEXT,
    source_uid TEXT,
    media_type TEXT,
    version_signature TEXT,
    source_title TEXT,
    subscription_id BIGINT,
    matched_subscription_ids TEXT,
    delivered_at BIGINT NOT NULL,
    archived_at BIGINT NOT NULL,
    archive_reason TEXT NOT NULL,
    merged_into_item_id BIGINT
);

CREATE TABLE IF NOT EXISTS item_genres (
    item_id BIGINT NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (item_id, genre_id),
    CONSTRAINT fk_item_genres_item FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS deliveries (
    id BIGSERIAL PRIMARY KEY,
    tg_user_id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    subscription_id BIGINT,
    delivered_at BIGINT NOT NULL,
    UNIQUE(tg_user_id, item_id),
    CONSTRAINT fk_deliveries_user FOREIGN KEY(tg_user_id) REFERENCES users(tg_user_id) ON DELETE CASCADE,
    CONSTRAINT fk_deliveries_item FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE,
    CONSTRAINT fk_deliveries_sub FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(tg_user_id);
CREATE INDEX IF NOT EXISTS idx_items_created_at ON items(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_items_source_uid ON items(source_uid);
CREATE INDEX IF NOT EXISTS idx_deliveries_user_item ON deliveries(tg_user_id, item_id);

ALTER TABLE users ADD COLUMN IF NOT EXISTS access_expires_at BIGINT;
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS country_codes TEXT NOT NULL DEFAULT '';
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS exclude_country_codes TEXT NOT NULL DEFAULT '';
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS content_filter TEXT NOT NULL DEFAULT 'any';
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS preset_key TEXT NOT NULL DEFAULT '';
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_countries TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS source_category_id TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS source_category_name TEXT;
ALTER TABLE items_archive ADD COLUMN IF NOT EXISTS source_category_id TEXT;
ALTER TABLE items_archive ADD COLUMN IF NOT EXISTS source_category_name TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS kinozal_id TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_original_language TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_vote_count INTEGER;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_status TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_age_rating TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_number_of_seasons INTEGER;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_number_of_episodes INTEGER;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_name TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_air_date TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_season_number INTEGER;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_episode_number INTEGER;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_name TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_air_date TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_season_number INTEGER;
ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_episode_number INTEGER;
ALTER TABLE items ADD COLUMN IF NOT EXISTS manual_bucket TEXT NOT NULL DEFAULT '';
ALTER TABLE items ADD COLUMN IF NOT EXISTS manual_country_codes TEXT NOT NULL DEFAULT '';
ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS matched_subscription_ids TEXT;
ALTER TABLE items ADD COLUMN IF NOT EXISTS source_release_text TEXT NOT NULL DEFAULT '';
ALTER TABLE items ADD COLUMN IF NOT EXISTS mal_id TEXT;
CREATE INDEX IF NOT EXISTS idx_users_access_state ON users(access_granted, access_expires_at);
CREATE INDEX IF NOT EXISTS idx_items_source_link ON items(source_link);
CREATE INDEX IF NOT EXISTS idx_items_media_source ON items(media_type, source_uid);

UPDATE items
SET kinozal_id = regexp_replace(source_uid, '^kinozal:([0-9]+)$', '\1')
WHERE (kinozal_id IS NULL OR kinozal_id = '')
  AND source_uid ~ '^kinozal:[0-9]+$';

UPDATE items
SET kinozal_id = regexp_replace(source_uid, '^.*details\.php\?id=([0-9]+).*$','\1')
WHERE (kinozal_id IS NULL OR kinozal_id = '')
  AND source_uid ~ 'details\.php\?id=[0-9]+';

UPDATE items
SET kinozal_id = regexp_replace(source_link, '^.*details\.php\?id=([0-9]+).*$','\1')
WHERE (kinozal_id IS NULL OR kinozal_id = '')
  AND COALESCE(source_link, '') ~ 'details\.php\?id=[0-9]+';

CREATE INDEX IF NOT EXISTS idx_items_kinozal_id ON items(kinozal_id);
CREATE INDEX IF NOT EXISTS idx_items_archive_kinozal_id ON items_archive(kinozal_id, archived_at DESC);
CREATE INDEX IF NOT EXISTS idx_deliveries_archive_user_kinozal ON deliveries_archive(tg_user_id, kinozal_id, delivered_at DESC);
CREATE TABLE IF NOT EXISTS muted_titles (
    id BIGSERIAL PRIMARY KEY,
    tg_user_id BIGINT NOT NULL,
    tmdb_id INTEGER NOT NULL,
    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
    UNIQUE (tg_user_id, tmdb_id)
);
ALTER TABLE users ADD COLUMN IF NOT EXISTS quiet_start_hour INTEGER;
ALTER TABLE users ADD COLUMN IF NOT EXISTS quiet_end_hour INTEGER;
CREATE TABLE IF NOT EXISTS pending_deliveries (
    id BIGSERIAL PRIMARY KEY,
    tg_user_id BIGINT NOT NULL,
    item_id BIGINT NOT NULL,
    matched_sub_ids TEXT NOT NULL DEFAULT '',
    old_release_text TEXT NOT NULL DEFAULT '',
    is_release_text_change INTEGER NOT NULL DEFAULT 0,
    queued_at BIGINT NOT NULL,
    UNIQUE (tg_user_id, item_id)
);
CREATE TABLE IF NOT EXISTS debounce_queue (
    tg_user_id BIGINT NOT NULL,
    kinozal_id TEXT NOT NULL,
    item_id BIGINT NOT NULL,
    matched_sub_ids TEXT NOT NULL DEFAULT '',
    deliver_after_ts BIGINT NOT NULL,
    reset_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE (tg_user_id, kinozal_id)
);
ALTER TABLE debounce_queue ADD COLUMN IF NOT EXISTS reset_count INTEGER NOT NULL DEFAULT 0;
