"""Flux WebSocket des shares: GET /v1/shares/stream[?from=<epoch>:<seq>][&address=<username>][&workername=<nom>].

Route branchée à la main (le générateur ne sait pas déclarer de WebSocket): documentée dans
openapi.yaml sous x-websocket-routes, incluse dans main.py.

Codes de fermeture:
  4400 paramètre invalide (curseur: format ou epoch inconnue; address/workername: vide)
  4408 client trop lent (buffer plein ou envoi bloqué): se reconnecter avec le dernier curseur reçu
  4410 curseur hors rétention: l'historique demandé n'existe plus, repartir sans `from`
  1013 trop de clients connectés
"""

import asyncio
import json
from contextlib import suppress
from datetime import datetime, timezone
from typing import Protocol

from fastapi import APIRouter, Query, WebSocket, WebSocketDisconnect

from pool_api.container import get_container
from pool_api.dao.ShareLog import Cursor
from pool_api.impl.v1.services.share_hub import (
    ShareFilter,
    ShareHub,
    Subscriber,
    TooManyClients,
    format_cursor,
    parse_cursor,
    share_event,
    watermark_event,
)
from settings import settings

CLOSE_OVERLOADED = 1013
CLOSE_BAD_CURSOR = 4400
CLOSE_SLOW_CONSUMER = 4408
CLOSE_OUT_OF_RETENTION = 4410

router = APIRouter()


class Socket(Protocol):
    async def send_text(self, data: str) -> None: ...
    async def receive(self) -> dict: ...
    async def close(self, code: int = 1000, reason: str | None = None) -> None: ...


class SlowConsumer(Exception):
    pass


class OutOfRetention(Exception):
    pass


async def _send(ws: Socket, text: str, timeout_s: float) -> None:
    try:
        await asyncio.wait_for(ws.send_text(text), timeout_s)
    except asyncio.TimeoutError:
        raise SlowConsumer from None


async def _replay(
    ws: Socket, hub: ShareHub, sub: Subscriber, cursor: Cursor, head: Cursor, send_timeout_s: float
) -> tuple[Cursor, datetime | None]:
    """Rejoue depuis la base, s'abonne au live, puis relit une dernière fois: les shares commités entre-temps
    sont soit dans cette relecture, soit dans la file (les doublons sont écartés par le curseur)."""
    last = cursor
    first = True
    attached: datetime | None = None
    is_attached = False
    while True:
        rows = await hub.dao.fetch_after(last, hub.batch)
        if first:
            first = False
            # seq est gapless: un trou juste après le curseur, ou des lignes qui existent mais ne sont plus là,
            # signifie que la rétention est passée par-dessus
            if rows and rows[0].epoch == cursor[0] and rows[0].seq != cursor[1] + 1:
                raise OutOfRetention
            if not rows and head > cursor:
                raise OutOfRetention
        for row in rows:
            # le curseur avance aussi sur les shares écartés: le filtre ne crée pas de trou à reprendre
            if sub.filter.matches(row):
                await _send(ws, share_event(row), send_timeout_s)
            last = row.cursor
        if len(rows) == hub.batch:
            continue
        if is_attached:
            return last, attached
        _, attached = hub.go_live(sub)
        is_attached = True


async def _live(ws: Socket, sub: Subscriber, last: Cursor, heartbeat_s: float, send_timeout_s: float) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + heartbeat_s
    while True:
        if sub.overflow:
            raise SlowConsumer
        remaining = deadline - loop.time()
        item = None
        if remaining > 0:
            with suppress(asyncio.TimeoutError):
                item = await asyncio.wait_for(sub.queue.get(), remaining)
        if item is None:
            deadline = loop.time() + heartbeat_s
            heartbeat = {"type": "heartbeat", "time": datetime.now(timezone.utc).isoformat()}
            await _send(ws, json.dumps(heartbeat), send_timeout_s)
            continue
        cursor, text = item
        if cursor is not None:
            if cursor <= last:  # déjà envoyé pendant le replay
                continue
            last = cursor
        await _send(ws, text, send_timeout_s)


async def _stream(
    ws: Socket, hub: ShareHub, sub: Subscriber, cursor: Cursor | None, head: Cursor,
    heartbeat_s: float, send_timeout_s: float,
) -> None:
    watermark = None
    if cursor is None:
        cursor, watermark = hub.go_live(sub)
        replay = False
    else:
        replay = True
    hello = {"type": "hello", "cursor": format_cursor(cursor), "head": format_cursor(head)}
    await _send(ws, json.dumps(hello), send_timeout_s)
    if replay:
        cursor, watermark = await _replay(ws, hub, sub, cursor, head, send_timeout_s)
    if watermark is not None:
        await _send(ws, watermark_event(watermark), send_timeout_s)
    await _live(ws, sub, cursor, heartbeat_s, send_timeout_s)


async def _wait_disconnect(ws: Socket) -> None:
    while (await ws.receive())["type"] != "websocket.disconnect":
        pass  # les messages du client sont ignorés


async def serve(
    ws: Socket,
    hub: ShareHub,
    cursor: Cursor | None,
    heartbeat_s: float | None = None,
    send_timeout_s: float | None = None,
    share_filter: ShareFilter | None = None,
) -> None:
    heartbeat_s = settings.share_stream_heartbeat_s if heartbeat_s is None else heartbeat_s
    send_timeout_s = settings.share_stream_send_timeout_s if send_timeout_s is None else send_timeout_s
    head = await hub.dao.head()
    if cursor is not None and cursor[0] > head[0]:
        await ws.close(CLOSE_BAD_CURSOR, "unknown epoch")
        return
    try:
        sub = hub.subscribe(share_filter)
    except TooManyClients:
        await ws.close(CLOSE_OVERLOADED, "too many clients")
        return
    stream = asyncio.create_task(_stream(ws, hub, sub, cursor, head, heartbeat_s, send_timeout_s))
    gone = asyncio.create_task(_wait_disconnect(ws))
    try:
        await asyncio.wait({stream, gone}, return_when=asyncio.FIRST_COMPLETED)
        if not stream.done():
            return  # le client est parti
        try:
            stream.result()
        except SlowConsumer:
            await _close(ws, CLOSE_SLOW_CONSUMER, "slow consumer")
        except OutOfRetention:
            await _close(ws, CLOSE_OUT_OF_RETENTION, "cursor out of retention")
        except (WebSocketDisconnect, RuntimeError):
            pass
    finally:
        hub.unsubscribe(sub)
        for task in (stream, gone):
            task.cancel()
        await asyncio.gather(stream, gone, return_exceptions=True)


async def _close(ws: Socket, code: int, reason: str) -> None:
    # un client bloqué peut aussi bloquer la fermeture
    with suppress(Exception):
        await asyncio.wait_for(ws.close(code, reason), 5)


@router.websocket("/v1/shares/stream")
async def stream_shares(
    ws: WebSocket,
    from_: str | None = Query(None, alias="from"),
    address: str | None = Query(None),
    workername: str | None = Query(None),
) -> None:
    # accept avant tout close: refuser avant le handshake donnerait un 403 sans code applicatif
    await ws.accept()
    if address == "" or workername == "":
        # un filtre vide ouvrirait silencieusement le flux complet
        await ws.close(CLOSE_BAD_CURSOR, "address and workername must not be empty")
        return
    cursor = None
    if from_ is not None:
        try:
            cursor = parse_cursor(from_)
        except ValueError:
            await ws.close(CLOSE_BAD_CURSOR, "invalid cursor, expected <epoch>:<seq>")
            return
    await serve(ws, get_container().share_hub, cursor, share_filter=ShareFilter(address, workername))
