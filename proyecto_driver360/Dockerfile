FROM python:3.11-slim

# Instalar dependencias
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/

CMD ["python", "scripts/anonimizacion_postgres.py"]