#!/usr/bin/env bash
# Copie le schema OpenAPI de chauff-cmn (version installée) en vendor/chauff-cmn.yaml,
# pour pouvoir le référencer par $ref depuis openapi.yaml.
# Interpréteur : $PYTHON, sinon venv/, sinon .venv/ (uv), sinon python3.
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -z "${PYTHON:-}" ]; then
  for candidate in venv/bin/python .venv/bin/python; do
    if [ -x "$candidate" ]; then PYTHON="$candidate"; break; fi
  done
fi
PYTHON="${PYTHON:-python3}"

SRC="$("$PYTHON" -c 'from importlib.resources import files; print(files("chauff_cmn") / "openapi" / "schema.yaml")')"

mkdir -p vendor
cp -f "$SRC" vendor/chauff-cmn.yaml
echo "vendor/chauff-cmn.yaml <- $SRC"
