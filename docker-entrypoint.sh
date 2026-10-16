#!/bin/sh
set -e

export DB_PASSWORD=$(cat /run/secrets/db_password)
export DATABASE_URL="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5432/${DB_NAME}"
export API_KEY=$(cat /run/secrets/pool_api_password)

export HISTORY_DATABASE_PASSWORD=$(cat /run/secrets/history_db_password)
export HISTORY_DATABASE_URL="postgresql://${HISTORY_DB_USER}:${HISTORY_DB_PASSWORD}@${HISTORY_DB_HOST}:5432/${HISTORY_DB_NAME}"


exec uvicorn pool_api.main:app --host 0.0.0.0 --port ${SERVER_PORT:-8000}
