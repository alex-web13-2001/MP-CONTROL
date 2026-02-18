---
description: Обновление архитектурной документации при изменениях кода
---

# Обновление документации

При КАЖДОМ изменении кода проекта, выполни следующие шаги:

## 1. Определи затронутые документы

| Что изменилось                                   | Документ                                  |
| ------------------------------------------------ | ----------------------------------------- |
| Таблицы PostgreSQL/ClickHouse (init.sql, models) | `docs/architecture/02_DATA_MODEL.md`      |
| Celery tasks (tasks.py, расписание)              | `docs/architecture/03_CELERY_PIPELINE.md` |
| API endpoints (app/api/)                         | `docs/architecture/04_BACKEND_API.md`     |
| Сервисы (app/services/)                          | `docs/architecture/05_SERVICES.md`        |
| Frontend (pages, components, stores)             | `docs/architecture/06_FRONTEND.md`        |
| Docker, env, Nginx                               | `docs/architecture/07_INFRASTRUCTURE.md`  |
| Архитектурные изменения                          | `docs/architecture/01_OVERVIEW.md`        |

## 2. Обнови документ

- Найди соответствующую секцию в документе
- Обнови таблицы, списки или диаграммы
- Если изменение крупное — обнови Mermaid-диаграммы

## 3. Добавь запись в Changelog документа

В конце документа добавь запись:

```markdown
### YYYY-MM-DD

- Краткое описание изменения
```

## 4. Обнови CHANGELOG.md проекта

Добавь запись в корневой `CHANGELOG.md` с описанием изменения документации.

## Важно

- Документация описывает ТЕКУЩЕЕ состояние кода, не планы
- Используй Mermaid для потоков данных
- Используй таблицы для перечислений
- НЕ дублируй код — описывай логику
