# Changelog

## 1.1.0 - 2026-04-15

- исправлено версионирование релизов, чтобы новые episode-progress версии не схлопывались в уже доставленные
- доставка, cooldown и история стали archive-aware для `deliveries_archive`
- quiet-hours pending и debounce стали устойчивее к изменению подписок между enqueue и flush
- архив доставок теперь хранит immutable snapshot отправленного item, а не зависит от позже мутировавшего `items`
- добавлен audit/repair tooling для поиска и безопасного replay пропущенных progress-апдейтов
- улучшено HTML/release follow-up форматирование и добавлен safe truncation
- расширен parsing episode progress, включая форматы вида `1x08`
