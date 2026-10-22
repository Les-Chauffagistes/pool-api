import os

import pytest


def infra_unavailable(reason: str):
    """Docker/Postgres absent: skip en local, échec en CI (sinon un runner sans Docker passerait au vert sans rien tester)."""
    if os.environ.get("CI"):
        pytest.fail(f"CI: {reason}", pytrace=False)
    pytest.skip(reason)
