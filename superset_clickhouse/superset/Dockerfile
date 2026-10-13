FROM apache/superset:6.1.0

USER root

COPY requirements.txt /tmp/requirements-extra.txt

# Начиная с Superset 4.1 образ использует uv и виртуальное окружение /app/.venv.
# Обычный `pip install` поставил бы пакет мимо окружения Superset.
RUN . /app/.venv/bin/activate && \
    uv pip install --no-cache -r /tmp/requirements-extra.txt

# Конфиг подхватывается автоматически из PYTHONPATH (/app/pythonpath).
# chmod нужен, чтобы пользователь superset мог прочитать файл
# независимо от прав, которые были у файла на хосте.
COPY superset_config.py /app/pythonpath/superset_config.py
RUN chmod 755 /app/pythonpath && chmod 644 /app/pythonpath/superset_config.py

USER superset