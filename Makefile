.PHONY: schema install test clean generate

install:
	uv venv --allow-existing
	uv pip install -r requirements.txt
	$(MAKE) schema

# Lien vers le schema OpenAPI de chauff-cmn, requis par les $ref de openapi.yaml
schema:
	./scripts/link-schema.sh

test: schema
	uv run pytest

clean:
	rm -rf vendor

generate: schema
	./scripts/generate.sh
