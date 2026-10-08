#!/usr/bin/env bash
# Génère les routes FastAPI (src/pool_api) à partir de openapi.yaml.
# Ce qui n'est pas à générer est listé dans .openapi-generator-ignore ; src/pool_api/impl/ n'est jamais écrasé.
# Les schémas de chauff-cmn ne sont PAS régénérés : ils sont importés depuis chauff_cmn.models.
set -euo pipefail
cd "$(dirname "$0")/.."

# Schémas partagés (noms dans chauff-cmn/openapi/schema.yaml) référencés par openapi.yaml
SHARED_SCHEMAS=(Pool PoolRuntime PoolHashrates PoolShares Hashrates)
SCHEMA_MAPPINGS=$(for s in "${SHARED_SCHEMAS[@]}"; do printf '%s=%s,' "$s" "$s"; done)

docker run --rm -u "$(id -u):$(id -g)" -v "$(pwd):/local" openapitools/openapi-generator-cli:v7.26.0 generate \
    -i /local/openapi.yaml \
    -g python-fastapi \
    -o /local \
    --additional-properties=packageName=pool_api \
    --schema-mappings "${SCHEMA_MAPPINGS%,}" >/dev/null

# --schema-mappings évite de générer les modèles mais laisse l'import vers pool_api.models.<x> :
# on le redirige vers la lib commune.
for s in "${SHARED_SCHEMAS[@]}"; do
  grep -rlE "^from pool_api\.models\.[a-z_]+ import $s( |$)" src 2>/dev/null \
    | xargs -I{} sed -i.bak -E "s/^from pool_api\.models\.[a-z_]+ import $s( .*)?$/from chauff_cmn.models import $s/" {} || true
done
find src -name '*.bak' -delete
echo "src/pool_api généré (modèles importés de chauff_cmn.models)"
