import os

# Ключ берётся из переменной окружения (см. .env)
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# Метаданные Superset: SQLite в persistent-томе (подходит для небольших установок)
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"
