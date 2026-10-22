import asyncio
from datetime import timedelta

import pytest
from fakes import T0, FakeConnector, FakeDAO, FakeWS, make_row

from pool_api.impl.v1.handlers.share_stream import serve
from pool_api.impl.v1.services.share_hub import ShareFilter, ShareHub, parse_cursor


async def new_hub(dao: FakeDAO, **kwargs) -> ShareHub:
    hub = ShareHub(dao, "postgresql://fake", connect=FakeConnector(), **kwargs)
    hub.position = await dao.head()
    return hub


def run(hub, ws, cursor, **kwargs) -> asyncio.Task:
    return asyncio.create_task(serve(ws, hub, cursor, heartbeat_s=60, send_timeout_s=1, **kwargs))


async def stop(task: asyncio.Task, ws: FakeWS) -> None:
    ws.disconnect()
    await asyncio.wait_for(task, 5)


def test_parse_cursor():
    assert parse_cursor("2:15") == (2, 15)
    for bad in ["", "5", "a:b", "1:2:3", "-1:4"]:
        with pytest.raises(ValueError):
            parse_cursor(bad)


async def test_replay_then_live_without_gap_or_duplicate():
    dao = FakeDAO([make_row(i) for i in range(1, 6)])
    hub = await new_hub(dao)
    ws = FakeWS()
    task = run(hub, ws, (1, 2))

    await ws.wait_for(lambda: ws.seqs() == [3, 4, 5])
    dao.add(make_row(6), make_row(7))
    await hub.pump()
    await ws.wait_for(lambda: ws.seqs() == [3, 4, 5, 6, 7])
    await hub.pump()  # rien de nouveau: pas de doublon
    dao.add(make_row(8))
    await hub.pump()
    await ws.wait_for(lambda: ws.seqs()[-1] == 8)

    assert ws.seqs() == [3, 4, 5, 6, 7, 8]
    assert ws.events[0] == {"type": "hello", "cursor": "1:2", "head": "1:5"}
    await stop(task, ws)


async def test_share_event_fields_hide_internals():
    dao = FakeDAO([make_row(1)])
    hub = await new_hub(dao)
    ws = FakeWS()
    task = run(hub, ws, (1, 0))
    await ws.wait_for(lambda: ws.seqs() == [1])
    assert ws.events[1] == {
        "type": "share",
        "share_id": "ckpool01:0000abcd/ckpool01.sharelog:100",
        "epoch": 1,
        "seq": 1,
        "instance": "ckpool01",
        "created_at": (T0 + timedelta(seconds=1)).isoformat(),
        "workinfoid": 7,
        "block_height": 0xABCD,
        "username": "bc1quser",
        "workername": "rig1",
        "diff": 1.0,
        "sdiff": 2.5,
        "result": True,
        "parse_error": False,
    }
    await stop(task, ws)


async def test_no_cursor_starts_live_without_replay():
    dao = FakeDAO([make_row(1), make_row(2)])
    hub = await new_hub(dao)
    ws = FakeWS()
    task = run(hub, ws, None)
    await ws.wait_for(lambda: ws.types() == ["hello"])
    dao.add(make_row(3))
    await hub.pump()
    await ws.wait_for(lambda: ws.seqs() == [3])
    await stop(task, ws)


async def test_resume_from_last_seen_cursor_after_reconnect():
    dao = FakeDAO([make_row(i) for i in range(1, 4)])
    hub = await new_hub(dao)
    first = FakeWS()
    task = run(hub, first, (1, 0))
    await first.wait_for(lambda: first.seqs() == [1, 2, 3])
    await stop(task, first)

    dao.add(make_row(4), make_row(5))
    await hub.pump()
    second = FakeWS()
    task = run(hub, second, (1, 3))
    await second.wait_for(lambda: second.seqs() == [4, 5])
    await stop(task, second)


async def test_replay_handoff_drops_events_already_replayed():
    # un share arrive dans la file du live alors qu'il a déjà été rejoué: il ne doit pas être renvoyé
    dao = FakeDAO([make_row(1), make_row(2)])
    hub = await new_hub(dao)
    ws = FakeWS()
    task = run(hub, ws, (1, 0))
    await ws.wait_for(lambda: ws.seqs() == [1, 2])
    sub = next(iter(hub._subs))
    sub.queue.put_nowait(((1, 2), "{}"))  # doublon d'un share déjà envoyé: écarté avant envoi (n'aurait pas de seq)
    dao.add(make_row(3))
    await hub.pump()
    await ws.wait_for(lambda: ws.seqs() == [1, 2, 3])
    assert ws.types().count("share") == 3
    await stop(task, ws)


async def test_replay_larger_than_one_batch():
    dao = FakeDAO([make_row(i) for i in range(1, 26)])
    hub = await new_hub(dao, batch=10)
    ws = FakeWS()
    task = run(hub, ws, (1, 0))
    await ws.wait_for(lambda: len(ws.seqs()) == 25)
    assert ws.seqs() == list(range(1, 26))
    await stop(task, ws)


async def test_cursor_across_epoch_change_replays_new_epoch():
    # failover: l'epoch 2 repart à des seq plus bas; le curseur (1, 9) reçoit tout l'epoch 2
    dao = FakeDAO([make_row(i) for i in range(1, 8)] + [make_row(6, epoch=2), make_row(7, epoch=2)])
    hub = await new_hub(dao)
    ws = FakeWS()
    task = run(hub, ws, (1, 7))
    await ws.wait_for(lambda: len(ws.seqs()) == 2)
    assert [(e["epoch"], e["seq"]) for e in ws.events if e["type"] == "share"] == [(2, 6), (2, 7)]
    await stop(task, ws)


async def test_slow_client_buffer_overflow_closes_4408():
    dao = FakeDAO([make_row(1)])
    hub = await new_hub(dao, buffer=3)
    ws = FakeWS()
    task = run(hub, ws, None)
    await ws.wait_for(lambda: ws.types() == ["hello"])
    ws.gate.clear()  # le client ne lit plus
    for seq in range(2, 12):
        dao.add(make_row(seq))
    await hub.pump()
    ws.gate.set()
    await asyncio.wait_for(task, 5)

    assert ws.closed is not None and ws.closed[0] == 4408
    assert not hub._subs  # désabonné
    # aucun trou silencieux: ce qui a été livré est un préfixe continu
    assert ws.seqs() == list(range(2, 2 + len(ws.seqs())))


async def test_stuck_send_times_out_with_4408():
    dao = FakeDAO([make_row(1)])
    hub = await new_hub(dao)
    ws = FakeWS()
    ws.gate.clear()
    task = run(hub, ws, (1, 0))
    await asyncio.wait_for(task, 5)
    assert ws.closed and ws.closed[0] == 4408


@pytest.mark.parametrize(
    "rows, cursor, ok",
    [
        ([10, 11, 12], (1, 3), False),  # 4..9 purgés
        ([10, 11, 12], (1, 0), False),
        ([10, 11, 12], (1, 9), True),  # juste avant le plus ancien: contigu
        ([10, 11, 12], (1, 12), True),  # à jour
        ([10, 11, 12], (1, 40), True),  # en avance: rien à rejouer, on attend
    ],
)
async def test_cursor_out_of_retention(rows, cursor, ok):
    dao = FakeDAO([make_row(i) for i in rows])
    hub = await new_hub(dao)
    ws = FakeWS()
    task = run(hub, ws, cursor)
    if ok:
        await ws.wait_for(lambda: "hello" in ws.types())
        await asyncio.sleep(0.05)
        assert ws.closed is None
        await stop(task, ws)
    else:
        await asyncio.wait_for(task, 5)
        assert ws.closed[0] == 4410
        assert ws.seqs() == []  # rien n'est rejoué à moitié


async def test_cursor_out_of_retention_when_everything_expired():
    dao = FakeDAO()
    dao.head = lambda: _async((1, 500))  # share_seq a avancé mais plus aucune ligne n'est retenue
    hub = await new_hub(dao)
    ws = FakeWS()
    await asyncio.wait_for(serve(ws, hub, (1, 20), heartbeat_s=60, send_timeout_s=1), 5)
    assert ws.closed[0] == 4410


async def _async(value):
    return value


async def test_unknown_epoch_closes_4400():
    hub = await new_hub(FakeDAO([make_row(1)]))
    ws = FakeWS()
    await asyncio.wait_for(serve(ws, hub, (9, 1)), 5)
    assert ws.closed[0] == 4400


async def test_too_many_clients_closes_1013():
    hub = await new_hub(FakeDAO([make_row(1)]), max_clients=1)
    first = FakeWS()
    task = run(hub, first, None)
    await first.wait_for(lambda: "hello" in first.types())
    second = FakeWS()
    await asyncio.wait_for(serve(second, hub, None), 5)
    assert second.closed[0] == 1013
    await stop(task, first)


async def test_heartbeat_is_periodic():
    hub = await new_hub(FakeDAO([make_row(1)]))
    ws = FakeWS()
    task = asyncio.create_task(serve(ws, hub, None, heartbeat_s=0.05, send_timeout_s=1))
    await ws.wait_for(lambda: ws.types().count("heartbeat") >= 2)
    await stop(task, ws)


async def test_watermark_follows_shares_and_never_regresses():
    dao = FakeDAO([make_row(1)])
    hub = await new_hub(dao)
    ws = FakeWS()
    task = run(hub, ws, None)
    await ws.wait_for(lambda: "hello" in ws.types())

    dao.add(make_row(2))
    dao.watermark_value = T0 + timedelta(seconds=2)
    await hub.pump()
    await ws.wait_for(lambda: "watermark" in ws.types())
    # le watermark arrive après les shares qu'il couvre
    assert ws.types() == ["hello", "share", "watermark"]

    dao.watermark_value = T0 + timedelta(seconds=1)  # une instance en retard réapparaît
    await hub.pump()
    dao.watermark_value = None  # plus aucune instance active
    await hub.pump()
    dao.watermark_value = T0 + timedelta(seconds=9)
    await hub.pump()
    await ws.wait_for(lambda: ws.types().count("watermark") == 2)
    marks = [e["watermark"] for e in ws.events if e["type"] == "watermark"]
    assert marks == [(T0 + timedelta(seconds=2)).isoformat(), (T0 + timedelta(seconds=9)).isoformat()]
    await stop(task, ws)


async def test_watermark_is_read_before_shares():
    dao = FakeDAO([make_row(1)])
    hub = await new_hub(dao)
    await hub.pump()
    assert dao.calls[0] == "watermark"


async def test_new_live_client_gets_current_watermark_after_replay():
    dao = FakeDAO([make_row(1), make_row(2)])
    dao.watermark_value = T0 + timedelta(seconds=2)
    hub = await new_hub(dao)
    await hub.pump()
    ws = FakeWS()
    task = run(hub, ws, (1, 0))
    await ws.wait_for(lambda: "watermark" in ws.types())
    assert ws.types() == ["hello", "share", "share", "watermark"]
    await stop(task, ws)


async def test_listen_reconnect_catches_up_without_gap_or_duplicate():
    dao = FakeDAO([make_row(1)])
    connector = FakeConnector()
    hub = ShareHub(dao, "postgresql://fake", connect=connector, poll_interval_s=0.02, reconnect_delay_s=0.02, keepalive_s=0.05)
    await hub.start()
    ws = FakeWS()
    task = run(hub, ws, None)
    try:
        await ws.wait_for(lambda: "hello" in ws.types())
        dao.add(make_row(2))
        connector.conns[0].notify()
        await ws.wait_for(lambda: ws.seqs() == [2])

        # coupure (HAProxy), la reconnexion échoue d'abord, des shares arrivent pendant ce temps
        connector.fail = 2
        connector.conns[0].kill()
        dao.add(make_row(3), make_row(4))
        while hub.listen_sessions < 2:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.1)  # laisse le premier tour de rattrapage se faire
        assert ws.seqs() == [2, 3, 4]
        dao.add(make_row(5))
        connector.conns[1].notify()
        await ws.wait_for(lambda: ws.seqs() == [2, 3, 4, 5])
        assert hub.listen_sessions == 2
        assert connector.conns[1].listeners[0][0] == "shares"
    finally:
        ws.disconnect()
        await asyncio.wait_for(task, 5)
        await hub.stop()


async def test_polling_delivers_without_any_notify():
    dao = FakeDAO([make_row(1)])
    hub = ShareHub(dao, "postgresql://fake", connect=FakeConnector(), poll_interval_s=0.02)
    await hub.start()
    ws = FakeWS()
    task = run(hub, ws, None)
    try:
        await ws.wait_for(lambda: "hello" in ws.types())
        dao.add(make_row(2))
        await ws.wait_for(lambda: ws.seqs() == [2])
    finally:
        ws.disconnect()
        await asyncio.wait_for(task, 5)
        await hub.stop()


def mixed_rows() -> list:
    return [
        make_row(1, username="bc1qAlice", workername="rig1"),
        make_row(2, username="bc1qbob", workername="rig1"),
        make_row(3, username="bc1qalice", workername="RIG2"),
        make_row(4, username=None, workername=None),  # parse_error
        make_row(5, username="bc1qalice", workername="rig1"),
    ]


@pytest.mark.parametrize(
    "share_filter, expected",
    [
        (ShareFilter(address="bc1qalice"), [3, 5]),  # sensible à la casse: bc1qAlice (seq 1) exclu
        (ShareFilter(address="BC1QALICE"), []),
        (ShareFilter(workername="Rig1"), [1, 2, 5]),
        (ShareFilter(address="bc1qalice", workername="rig2"), [3]),
        (ShareFilter(address="inconnu"), []),
        (ShareFilter(), [1, 2, 3, 4, 5]),
    ],
)
async def test_filter_applies_to_replay_and_live(share_filter, expected):
    dao = FakeDAO(mixed_rows())
    hub = await new_hub(dao)
    ws = FakeWS()
    task = run(hub, ws, (1, 0), share_filter=share_filter)
    await ws.wait_for(lambda: "hello" in ws.types())

    dao.add(
        make_row(6, username="BC1QALICE", workername="rig1"),
        make_row(7, username="bc1qbob", workername="rig2"),
    )
    await hub.pump()
    live = [s for s in (6, 7) if share_filter.matches(dao.rows[s - 1])]  # seq 6: BC1QALICE, 7: bc1qbob
    await ws.wait_for(lambda: ws.seqs() == expected + live)
    await stop(task, ws)


async def test_filtered_client_does_not_consume_buffer_with_other_shares():
    dao = FakeDAO([make_row(1)])
    hub = await new_hub(dao, buffer=3)
    ws = FakeWS()
    task = run(hub, ws, None, share_filter=ShareFilter(address="bc1qalice"))
    await ws.wait_for(lambda: ws.types() == ["hello"])
    ws.gate.clear()
    for seq in range(2, 12):  # 10 shares d'un autre user: le buffer de 3 ne doit pas déborder
        dao.add(make_row(seq, username="bc1qbob"))
    await hub.pump()
    ws.gate.set()
    dao.add(make_row(12, username="bc1qalice"))
    await hub.pump()
    await ws.wait_for(lambda: ws.seqs() == [12])
    assert ws.closed is None
    await stop(task, ws)


async def test_filtered_resume_does_not_replay_skipped_shares():
    dao = FakeDAO(mixed_rows())
    hub = await new_hub(dao)
    ws = FakeWS()
    task = run(hub, ws, (1, 1), share_filter=ShareFilter(address="bc1qalice"))
    await ws.wait_for(lambda: ws.seqs() == [3, 5])
    await stop(task, ws)
