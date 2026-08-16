FROM python:3.10-slim

WORKDIR /app

# Dépendances système
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copie les requirements du simulateur
COPY simulator/requirements.txt /app/simulator/requirements.txt
RUN pip install --no-cache-dir -r /app/simulator/requirements.txt

# Copie le code du simulateur
COPY simulator/ /app/simulator/
COPY .env /app/.env

CMD ["python", "-m", "simulator.main"]