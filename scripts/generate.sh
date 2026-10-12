#!/usr/bin/env bash
# Génère les routes FastAPI et les modèles (src/pool_api) à partir de openapi.yaml.
# Ce qui n'est pas à générer est listé dans .openapi-generator-ignore ; src/pool_api/impl/ n'est jamais écrasé.
# Les schémas de chauff-cmn (vendor/chauff-cmn.yaml, source de vérité) sont régénérés localement dans pool_api.models.
set -euo pipefail
cd "$(dirname "$0")/.."

docker run --rm -u "$(id -u):$(id -g)" -v "$(pwd):/local" openapitools/openapi-generator-cli:v7.26.0 generate \
    -i /local/openapi.yaml \
    -g python-fastapi \
    -o /local \
    --additional-properties=packageName=pool_api >/dev/null
echo "src/pool_api généré"
