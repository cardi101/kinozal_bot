# Kinozal News Bot Test Refactor

Тестовый стенд для безопасного рефакторинга Kinozal News Bot отдельно от прода.

## Статус

- текущая рабочая ветка: `refactor/phase-1`
- стабильная точка рефакторинга: `refactor-phase-2-stable`

## Что это за репозиторий

Этот репозиторий используется как отдельный тестовый стенд, чтобы:
- распиливать большой `app.py` на модули без риска для прода
- проверять startup / shutdown / polling / TMDB enrichment
- валидировать infra split (`db.py`, `tmdb_client.py`, `kinozal_source.py`, `redis_cache.py`)
- фиксировать стабильные точки рефакторинга через git и теги

## Документация

- [Архитектура проекта](./ARCHITECTURE.md)
- [Снимок состояния handoff](./HANDOFF_STATUS.txt)

## Быстрый запуск

```bash
cp .env.example .env
nano .env
docker compose up -d --build
docker compose logs -f app
