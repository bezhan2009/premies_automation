# ---------- BUILD STAGE ----------
FROM python:3.11-slim AS builder

WORKDIR /app

# Копируем requirements
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь проект
COPY . .

# ---------- RUNTIME STAGE ----------
FROM python:3.11-slim

WORKDIR /root/

# Копируем зависимости из builder
COPY --from=builder /usr/local/lib/python3.11 /usr/local/lib/python3.11

# Создаём директорию для конфигов
RUN mkdir configs

# Копируем код
COPY --from=builder /app/main.py .

# Копируем конфиги YAML
COPY --from=builder /app/configs/docker/configs.yaml ./configs/

# Копируем .env
COPY --from=builder /app/.env .

EXPOSE 50051

CMD ["python", "main.py"]
