import os
import subprocess
import sys
from pathlib import Path

import pytest

# settings.py exige ces variables à l'import; aucun test ne s'y connecte (les tests Postgres ont leur conteneur)
os.environ.setdefault("DATABASE_URL", "postgresql://unused")
os.environ.setdefault("HISTORY_DATABASE_URL", "postgresql://unused")
os.environ.setdefault("PROMETHEUS_URL", "http://unused")
os.environ.setdefault("API_KEY", "unused")

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session", autouse=True)
def chauff_cmn_schema():
    """Crée vendor/chauff-cmn.yaml (cible des $ref de openapi.yaml), y compris en CI."""
    subprocess.run(
        [str(ROOT / "scripts" / "link-schema.sh")],
        check=True,
        env={**os.environ, "PYTHON": sys.executable},
    )
    return ROOT / "vendor" / "chauff-cmn.yaml"
