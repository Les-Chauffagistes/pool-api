import asyncio
import json

import pytest

from pool_api.impl.v1.services.ping_monitor import CkHost, PingMonitor


def test_ck_host_parses_address():
    host = CkHost(name="a", address="10.0.0.1:3333")
    assert (host.ip, host.port) == ("10.0.0.1", 3333)


@pytest.fixture
async def tcp_server():
    server = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
    yield server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()


def write_config(tmp_path, hosts) -> str:
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"ck_hosts": hosts}))
    return str(path)


async def closed_port() -> int:
    server = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    server.close()
    await server.wait_closed()
    return port


async def test_refresh_reports_online_and_offline_hosts(tmp_path, tcp_server):
    dead = await closed_port()
    config = write_config(
        tmp_path,
        [{"name": "up", "address": f"127.0.0.1:{tcp_server}"}, {"name": "down", "address": f"127.0.0.1:{dead}"}],
    )
    monitor = PingMonitor(config, interval_s=60)
    assert monitor.latest is None
    await monitor.start()
    try:
        ping = await monitor.refresh()
    finally:
        await monitor.stop()

    assert (ping.total, ping.online, ping.offline) == (2, 1, 1)
    up, down = ping.hosts
    assert up.online and up.latency_ms >= 0 and "tcp connect" in up.raw
    assert not down.online and down.latency_ms == 0
    assert monitor.latest is ping


async def test_unreachable_host_times_out(tmp_path, monkeypatch):
    async def hang(*a, **kw):
        await asyncio.sleep(10)

    monkeypatch.setattr(asyncio, "open_connection", hang)
    monitor = PingMonitor(write_config(tmp_path, [{"name": "slow", "address": "10.255.255.1:3333"}]), 60, timeout_s=0.05)
    await monitor.start()
    try:
        ping = await monitor.refresh()
    finally:
        await monitor.stop()
    assert ping.offline == 1 and "Timeout" in ping.hosts[0].raw


async def test_background_loop_populates_latest_and_survives_errors(tmp_path, tcp_server, monkeypatch):
    monitor = PingMonitor(write_config(tmp_path, [{"name": "up", "address": f"127.0.0.1:{tcp_server}"}]), 0.01)
    real_refresh, calls = monitor.refresh, 0

    async def flaky():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        return await real_refresh()

    monkeypatch.setattr(monitor, "refresh", flaky)
    await monitor.start()
    for _ in range(100):
        if monitor.latest is not None:
            break
        await asyncio.sleep(0.01)
    await monitor.stop()
    assert monitor.latest is not None and monitor.latest.online == 1


async def test_stop_without_start_is_a_noop(tmp_path):
    await PingMonitor(write_config(tmp_path, []), 1).stop()
