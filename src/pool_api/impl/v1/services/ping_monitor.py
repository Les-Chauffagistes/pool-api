import asyncio
import logging
import time
from contextlib import suppress
from datetime import datetime

# pyrefly: ignore [missing-source-for-stubs]
from aiofiles import open
from pydantic import BaseModel

from pool_api.models.pool_ping import PoolHost, PoolPing

logger = logging.getLogger(__name__)


class CkHost(BaseModel):
    name: str
    address: str

    @property
    def ip(self) -> str:
        return self.address.rsplit(":", 1)[0]

    @property
    def port(self) -> int:
        return int(self.address.rsplit(":", 1)[1])


class CkConfig(BaseModel):
    ck_hosts: list[CkHost]


class PingMonitor:
    """Sonde périodiquement les hôtes ck et garde en mémoire le dernier résultat."""

    def __init__(self, config_path: str, interval_s: float, timeout_s: float = 3.0):
        self.config_path = config_path
        self.interval_s = interval_s
        self.timeout_s = timeout_s
        self._hosts: list[CkHost] = []
        self._latest: PoolPing | None = None
        self._task: asyncio.Task[None] | None = None

    @property
    def latest(self) -> PoolPing | None:
        return self._latest

    async def start(self) -> None:
        async with open(self.config_path) as f:
            self._hosts = CkConfig.model_validate_json(await f.read()).ck_hosts
        self._task = asyncio.create_task(self._run(), name="ping-monitor")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task
        self._task = None

    async def refresh(self) -> PoolPing:
        hosts = await asyncio.gather(*(self._probe_host(h) for h in self._hosts))
        online = sum(h.online for h in hosts)
        self._latest = PoolPing(
            updated_at=datetime.now().isoformat(),
            total=len(hosts),
            online=online,
            offline=len(hosts) - online,
            hosts=list(hosts),
        )
        return self._latest

    async def _run(self) -> None:
        while True:
            try:
                await self.refresh()
            except Exception:  # une erreur ponctuelle ne doit pas tuer la boucle
                logger.exception("Échec du ping des hôtes ck")
            await asyncio.sleep(self.interval_s)

    async def _probe_host(self, host: CkHost) -> PoolHost:
        start = time.perf_counter()
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(host.ip, host.port), timeout=self.timeout_s
            )
        except (OSError, asyncio.TimeoutError) as e:
            return PoolHost(name=host.name, online=False, latencyMs=0, raw=repr(e))

        latency_ms = round((time.perf_counter() - start) * 1000, 2)
        writer.close()
        await writer.wait_closed()
        return PoolHost(
            name=host.name,
            online=True,
            latencyMs=latency_ms,
            raw=f"tcp connect {host.address} in {latency_ms} ms",
        )
