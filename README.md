# MMS - Marketplace Management System

Инструмент для автоматизации рекламы и глубокой финансовой аналитики на основе исторических данных маркетплейсов.

## 🚀 Технологический стек

### Backend

- **Python 3.11+** — основной язык
- **FastAPI** — асинхронный веб-фреймворк с автодокументацией Swagger
- **Celery + Redis** — фоновые задачи для загрузки данных
- **curl_cffi** — HTTP клиент для обхода защиты WB (JA3 Fingerprinting)

### Databases

- **PostgreSQL 15+** — структурные данные (пользователи, магазины, настройки)
- **ClickHouse** — аналитика (заказы, продажи, рекламные показы)

### Frontend

- **React 18 + TypeScript** — UI библиотека
- **Vite** — сборщик
- **Zustand** — state management
- **TanStack Query** — data fetching и кэширование
- **Recharts** — графики и визуализация

### Infrastructure

- **Docker & Docker Compose** — контейнеризация
- **Nginx** — reverse proxy

## 📦 Быстрый старт

### Требования

- Docker Desktop >= 4.0
- Docker Compose >= 2.0

### Запуск

1. **Клонируйте репозиторий:**

```bash
git clone https://github.com/your-username/mp-control.git
cd mp-control
```

2. **Создайте файл окружения:**

```bash
cp .env.example .env
# Отредактируйте .env при необходимости
```

3. **Запустите все сервисы:**

```bash
docker-compose up --build
```

4. **Откройте в браузере:**

- Frontend: http://localhost
- API Docs (Swagger): http://localhost/api/docs
- API ReDoc: http://localhost/api/redoc

## 🏗️ Структура проекта

```
MP-CONTROL/
├── backend/           # FastAPI Backend
│   ├── app/           # Основное приложение
│   │   ├── api/       # API endpoints
│   │   ├── core/      # Ядро (DB connections)
│   │   ├── models/    # SQLAlchemy models
│   │   ├── schemas/   # Pydantic schemas
│   │   └── services/  # Бизнес-логика
│   └── celery_app/    # Celery workers
├── frontend/          # React + TypeScript + Vite
│   ├── src/
│   │   ├── api/       # API клиент
│   │   ├── components/# React компоненты
│   │   ├── pages/     # Страницы
│   │   ├── stores/    # Zustand stores
│   │   └── hooks/     # Custom hooks
├── nginx/             # Nginx конфигурация
├── docker/            # Docker конфигурации
│   ├── postgres/      # PostgreSQL init scripts
│   └── clickhouse/    # ClickHouse init scripts
└── docker-compose.yml # Оркестрация сервисов
```

## 🔧 Разработка

### Backend (отдельно)

```bash
cd backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Frontend (отдельно)

```bash
cd frontend
npm install
npm run dev
```

### Celery worker

```bash
cd backend
celery -A celery_app.celery worker --loglevel=info
```

## 📊 API Endpoints

| Endpoint         | Метод | Описание        |
| ---------------- | ----- | --------------- |
| `/api/health`    | GET   | Health check    |
| `/api/v1/`       | GET   | API root        |
| `/api/v1/status` | GET   | Статус сервисов |
| `/api/docs`      | GET   | Swagger UI      |
| `/api/redoc`     | GET   | ReDoc           |

## 🗄️ Базы данных

### PostgreSQL

Хранит структурные данные:

- Пользователи и авторизация
- Магазины и API ключи
- Настройки автобиддера

### ClickHouse

Хранит аналитические данные:

- История заказов (6+ месяцев)
- Статистика рекламы
- Позиции товаров

## 📝 Лицензия

MIT
