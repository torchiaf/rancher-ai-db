FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libssl-dev libffi-dev python3-dev cargo \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY main.py /app/main.py

RUN python -m pip install --upgrade pip \
 && pip install --no-cache-dir redis pymysql cryptography websockets asyncio

CMD ["python", "/app/main.py"]