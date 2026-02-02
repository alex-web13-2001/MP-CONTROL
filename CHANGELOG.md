# Changelog

Все изменения в проекте документируются в этом файле.

## [Unreleased] - 2026-02-02

### Added

- **Direct JSON Ingestion:** Добавлен метод `parse_json_rows` в `WBReportParser` для прямой обработки данных из API V5, минуя CSV.
- **Sequential Sync Logic:** Реализован строгий последовательный порядок запросов ("Правило одной руки") с паузами 5с между неделями и 60с перед первым опросом (хотя опрос больше не нужен).
- **Safe Logging:** Внедрена вложенная транзакция (`begin_nested`) для логирования в `MarketplaceClient`, чтобы ошибки вставки логов (например, FK violation) не прерывали основной бизнес-процесс.
- **Troubleshooting Guide:** Добавлен раздел в `walkthrough.md` по решению проблем с API лимитами и 429 ошибками.

### Changed

- **WB Finance Sync:** Полностью переписан механизм синхронизации фин. отчетов (`WBFinanceReportService`).
  - **Отключено:** Генерация отчетов через `/api/v1/reports/financial/generate` (API возвращал 404).
  - **Включено:** Получение данных напрямую из метода `/api/v5/supplier/reportDetailByPeriod`.
- **Infrastructure:**
  - Обновлен `docker-compose.yml`: добавлены healthchecks, исправлены env var для ClickHouse (`CLICKHOUSE_DB` вместо `CLICKHOUSE_DATABASE`).
  - Исправлен `init.sql`: восстановлено создание таблицы `fact_finances` и `fact_finances_latest`.
- **Celery Tasks:**
  - `sync_wb_finance_3months`: переведена на использование нового метода `get_report_data`.
  - Добавлен автоматический реконнект к Redis в `RedisRateLimiter` и `CircuitBreaker` для исправления ошибки `RuntimeError: Event loop is closed`.

### Fixed

- **API 404 Error:** Устранена ошибка при попытке генерации отчетов по старому методу.
- **Event Loop Error:** Исправлено падение воркеров Celery из-за закрытия event loop при использовании `asyncio.run`.
- **Auth Error:** Исправлена проблема с пользователем `default` в ClickHouse (удален конфликтный `default-user.xml`).
