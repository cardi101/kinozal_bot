# Kinozal News Bot Test — Architecture

## Purpose
Тестовый стенд для безопасного рефакторинга Kinozal News Bot отдельно от прода.

## Runtime
- запуск через `docker compose`
- bot app в контейнере `app`
- TorAPI отдельно
- PostgreSQL внешний контейнер
- Redis внешний инстанс

## Entry point
- `app.py` — composition root
  - собирает зависимости
  - инициализирует DB / cache / tmdb / source
  - регистрирует handlers
  - запускает runtime

## Main layers

### Infra
- `db.py` — доступ к PostgreSQL
- `redis_cache.py` — кеш
- `tmdb_client.py` — TMDB API
- `kinozal_source.py` — получение и нормализация source feed

### Runtime
- `runtime_app.py` — lifecycle приложения, startup/shutdown
- `runtime_poller.py` — polling / processing loop

### Handlers
- `menu_handlers.py`
- `subscription_basic_handlers.py`
- `subscription_filter_handlers.py`
- `subscription_input_handlers.py`
- `subscription_wizard_handlers.py`
- `subscription_test_handlers.py`
- `user_handlers.py`
- `admin_match_handlers.py`
- `admin_access_handlers.py`

### View / keyboard
- `menu_views.py`
- `keyboards.py`

### Domain / helper modules
- `parsing_basic.py`
- `parsing_audio.py`
- `text_access.py`
- `source_categories.py`
- `release_versioning.py`
- `country_helpers.py`
- `item_years.py`
- `media_detection.py`
- `keyword_filters.py`
- `title_prep.py`
- `match_text.py`
- `tmdb_aliases.py`
- `content_buckets.py`
- `tmdb_match_validation.py`
- `subscription_presets.py`
- `genres_helpers.py`
- `subscription_matching.py`
- `subscription_text.py`
- `delivery_formatting.py`
- `delivery_sender.py`
- `service_helpers.py`
- `source_health.py`
- `admin_helpers.py`
- `config.py`
- `states.py`
- `utils.py`

## Stable point
Тег стабильной точки:
- `refactor-phase-2-stable`

## Important pitfalls already found
- нельзя делать механическую замену `db.` -> `self.db.` без проверки строковых URL
- проверки psycopg надо делать внутри контейнера `app`
- если пропали постеры, сначала проверять `tmdb_poster_url` и TMDB enrichment
- внешний PostgreSQL живёт отдельно от compose app stack

## Recommended next steps
1. держать `app.py` тонким composition root
2. не смешивать infra и handlers обратно
3. любые новые split-изменения проверять через:
   - `docker compose exec -T app python -m compileall /app`
   - import smoke
   - `docker compose logs --tail=120 app`
