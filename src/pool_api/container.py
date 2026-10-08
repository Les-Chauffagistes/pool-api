from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # évite un import circulaire : les handlers importent ce module
    from pool_api.impl.v1.services.pool import PoolService


@dataclass(frozen=True)
class Container:
    """Services de l'application, construits une fois au démarrage (cf. main.py)."""

    pool_service: "PoolService"


_container: Container | None = None


def set_container(container: Container | None) -> None:
    global _container
    _container = container


def get_container() -> Container:
    if _container is None:
        raise RuntimeError("Container non initialisé : le lifespan de l'application n'a pas démarré")
    return _container
