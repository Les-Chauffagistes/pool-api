import os
import subprocess
import sys
from pathlib import Path

import pytest

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
