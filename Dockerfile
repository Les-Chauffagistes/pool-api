FROM python:3.13-slim AS builder

WORKDIR /app

# chauff-cmn vient de l'index privé, fourni via le build secret "pipindex" (jamais écrit dans une couche)
COPY requirements.txt .
RUN --mount=type=secret,id=pipindex \
    PIP_EXTRA_INDEX_URL="$(cat /run/secrets/pipindex 2>/dev/null || true)" \
    pip install --no-cache-dir --require-hashes --only-binary :all: --trusted-host 172.16.0.1 -r requirements.txt


FROM python:3.13-slim AS runtime

WORKDIR /app

# stdout non bufferisé : sinon les logs de l'app restent coincés sous Swarm (pas de TTY)
# PYTHONPATH : le code est en layout src/ (pool_api/ et settings.py) et n'est pas installé comme package
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/* \
    && addgroup --system app && adduser --system --ingroup app app

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# config.json est lu en chemin relatif (cwd) par PingMonitor
COPY --chown=app:app config.json ./
COPY --chown=app:app src ./src
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

USER app

EXPOSE ${SERVER_PORT:-8000}

HEALTHCHECK --interval=15s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -fs http://localhost:${SERVER_PORT:-8000}/health || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]