# ClickHouse + Superset

Версии: ClickHouse 26.9, Apache Superset 6.1.0, clickhouse-connect >= 0.13.0.

## Структура

```
.
├── docker-compose.yml
├── .env.example
└── superset/
    ├── Dockerfile
    ├── requirements.txt
    └── superset_config.py
```

## Запуск

```bash
cp .env.example .env
# отредактируйте .env: SUPERSET_SECRET_KEY (openssl rand -base64 42) и пароли
docker compose up -d --build
docker compose ps          # superset-init должен завершиться с Exited (0)
```

Если до этого запускалась старая версия, сначала сотрите тома:

```bash
docker compose down -v
```

Superset: http://<IP>:8088 (логин/пароль из .env).

## Подключение ClickHouse в Superset

Settings -> Database Connections -> + Database -> ClickHouse Connect.
URI (хост - имя сервиса (clickhouse), не localhost):

```
clickhousedb://default:<CLICKHOUSE_PASSWORD>@clickhouse:8123/default
```

## Порты

| Порт | Сервис |
|------|--------|
| 8088 | Superset UI |
| 8123 | ClickHouse HTTP |
| 9000 | ClickHouse native |

Если ClickHouse не нужен снаружи - удалите его секцию `ports` и не открывайте 8123/9000 в файрволе.
