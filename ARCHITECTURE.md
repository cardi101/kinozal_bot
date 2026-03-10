cd /opt/kinozal-news-bot-test/kinozal-news-bot-test

cat > ARCHITECTURE.md <<'MD'
# Kinozal News Bot Test Refactor — Architecture

## Назначение

Этот репозиторий — отдельный тестовый стенд для безопасного рефакторинга Kinozal News Bot без риска для продового окружения.

Основная цель:
- распилить исторически большой `app.py` на понятные модули
- вынести infra-слой, runtime-слой и handler-слой
- сохранить рабочий polling, TMDB enrichment, доставку уведомлений и постеров
- фиксировать стабильные точки рефакторинга через git-ветки и теги

## Текущее состояние

- основная рабочая ветка рефакторинга: `refactor/phase-1`
- стабильная точка: `refactor-phase-2-stable`
- `app.py` приведён к роли тонкого composition root
- runtime и infra уже в основном вынесены в отдельные модули

## Runtime topology

### Контейнеры и внешние зависимости

Приложение запускается через `docker compose`.

Основные части:
- `app` — основной контейнер Telegram-бота
- `torapi` — отдельный контейнер для источника / API-прокси
- PostgreSQL — внешний контейнер / внешний инстанс, не встроенный в текущий compose app stack
- Redis — внешний инстанс

### Основные зависимости окружения

Ключевые env-параметры:
- `BOT_TOKEN`
- `ADMIN_IDS`
- `TMDB_TOKEN`
- `DATABASE_URL`
- `REDIS_URL`
- `TORAPI_BASE`
- `BOOTSTRAP_AS_READ`
- `SOURCE_FETCH_LIMIT`
- `TMDB_CACHE_TTL`
- `TMDB_NEGATIVE_CACHE_TTL`
- `SOURCE_ERROR_ALERT_THRESHOLD`
- `SOURCE_ERROR_ALERT_REPEAT_MINUTES`

## Entry point

### `app.py`

`app.py` — это composition root.

Он отвечает за:
- импорт основных модулей
- создание инфраструктурных зависимостей
- сборку `db`, `cache`, `tmdb`, `source`
- регистрацию handlers
- передачу зависимостей в runtime
- запуск приложения

`app.py` **не должен снова превращаться** в место, где живут:
- большие helper-функции
- infra-классы
- бизнес-логика матчинга
- логика доставки
- крупные handler-реализации

## Архитектурные слои

## 1. Infra layer

Infra-слой отвечает за работу с внешними системами.

### `db.py`
Ответственность:
- подключение к PostgreSQL
- выполнение запросов
- CRUD-операции по пользователям, подпискам, релизам, доставке, служебным данным
- выдача агрегированных данных для handlers и runtime

### `redis_cache.py`
Ответственность:
- кеширование TMDB и связанных lookup-данных
- negative cache для неудачных TMDB-поисков
- снижение лишних сетевых запросов

### `tmdb_client.py`
Ответственность:
- поиск фильмов / сериалов в TMDB
- enrichment карточек релизов
- получение постеров и метаданных
- работа с language / cache policy

Важно:
- base URL должен быть `https://api.themoviedb.org/3`
- домен постеров должен быть `https://image.tmdb.org/...`

### `kinozal_source.py`
Ответственность:
- запрос данных из источника
- получение RSS / JSON / API-ответов
- базовая нормализация входных данных
- подготовка source items для дальнейшей обработки

## 2. Runtime layer

Runtime-слой отвечает за жизненный цикл приложения.

### `runtime_app.py`
Ответственность:
- startup
- shutdown
- создание / остановка фоновых задач
- корректное завершение polling и worker-задач
- безопасная обработка `asyncio.CancelledError`

### `runtime_poller.py`
Ответственность:
- основной polling-цикл
- получение новых элементов из source
- запуск обработки новых релизов
- orchestration между source, TMDB, DB и delivery

## 3. Handler layer

Handler-слой отвечает за Telegram-взаимодействие с пользователем и админом.

### Меню и пользовательский поток
- `menu_views.py`
- `menu_handlers.py`
- `user_handlers.py`

### Подписки
- `subscription_basic_handlers.py`
- `subscription_filter_handlers.py`
- `subscription_input_handlers.py`
- `subscription_wizard_handlers.py`
- `subscription_test_handlers.py`

### Админские сценарии
- `admin_match_handlers.py`
- `admin_access_handlers.py`

### Клавиатуры
- `keyboards.py`

## 4. Domain / helper layer

Это слой прикладной логики, нормализации текста, матчинга и форматирования.

### Базовые и системные helpers
- `config.py` — конфигурация из env
- `states.py` — FSM / состояния сценариев
- `utils.py` — общие утилиты

### Парсинг и нормализация
- `parsing_basic.py`
- `parsing_audio.py`
- `title_prep.py`
- `match_text.py`
- `text_access.py`

### Категоризация и определение типа контента
- `source_categories.py`
- `content_buckets.py`
- `media_detection.py`
- `country_helpers.py`
- `genres_helpers.py`
- `item_years.py`
- `keyword_filters.py`

### Версионирование и извлечение данных из релиза
- `release_versioning.py`

### TMDB match-логика
- `tmdb_aliases.py`
- `tmdb_match_validation.py`

### Подписки и совпадения
- `subscription_presets.py`
- `subscription_matching.py`
- `subscription_text.py`

### Доставка и сервисные части
- `delivery_formatting.py`
- `delivery_sender.py`
- `service_helpers.py`
- `source_health.py`
- `admin_helpers.py`

## Поток обработки нового релиза

Ниже — упрощённая схема жизненного цикла одного нового элемента.

1. `runtime_poller.py` получает данные из `kinozal_source.py`
2. source item проходит базовую нормализацию
3. определяется media type / category / bucket
4. если элемент video-like, запускается логика матчинга
5. `tmdb_client.py` пытается найти TMDB match
6. результат валидируется через `tmdb_match_validation.py`
7. итоговые данные сохраняются в БД через `db.py`
8. вычисляются совпадения по пользовательским подпискам
9. `delivery_formatting.py` собирает сообщение
10. `delivery_sender.py` отправляет уведомление пользователю
11. state / delivery info / dedupe info сохраняются в БД

## Почему split был сделан именно так

Исходно `app.py` был перегружен несколькими ролями одновременно:
- entry point
- infra factory
- storage access layer
- source integration
- runtime orchestration
- handler registry
- часть доменной логики

Это делало файл тяжёлым для:
- чтения
- безопасного изменения
- тестирования
- передачи внешним разработчикам

Текущий split разделяет ответственности:
- infra отдельно
- runtime отдельно
- handlers отдельно
- domain/helpers отдельно
- `app.py` только собирает приложение

## Важные найденные грабли

## 1. Нельзя делать механическую замену `db.` -> `self.db.`
Во время split это уже ломало строковые URL внутри `tmdb_client.py`.

Из-за этого были повреждены:
- `api.themoviedb.org`
- `image.tmdb.org`

Любые массовые замены по классу надо проверять вручную.

## 2. Проверки PostgreSQL / psycopg надо делать внутри контейнера `app`
На хосте системный Python может не иметь нужных зависимостей.

Правильный формат проверки:
```bash
docker compose exec -T app python - <<'PY'
# code
PY
