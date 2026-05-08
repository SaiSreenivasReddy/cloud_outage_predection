#!/bin/bash
# airflow/init.sh — Airflow 3 one-time initialisation
set -e

echo "[airflow-init] Running DB migrations..."
airflow db migrate

echo "[airflow-init] Setting up admin password for Simple Auth Manager..."
echo '{"admin": "admin"}' > /opt/airflow/data/passwords.json

echo "[airflow-init] Registering postgres_default connection..."
CONN_URI="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@db:5432/${POSTGRES_DB}"
airflow connections add postgres_default \
  --conn-uri "$CONN_URI" \
  || echo "Connection already exists — skipping."

echo "[airflow-init] Done ✅"
