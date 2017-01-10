import asyncio
import logging
import os
from calendar import timegm
from datetime import datetime
from typing import Optional, Set, Tuple  # pylint: disable=unused-import

import jwt
from aiohttp import web

from crispywaffle.client import ClientQueue, match_client_queue

LISTEN_SECRET = os.environ.get("LISTEN_SECRET")
SEND_SECRET = os.environ.get("SEND_SECRET")

print(jwt.encode({}, LISTEN_SECRET, algorithm='HS256'))
print(jwt.encode({}, SEND_SECRET, algorithm='HS256'))

CRISPY_LOGGER = logging.getLogger("crispy")

SHUTDOWN = asyncio.Event()


def get_utc_timestamp() -> int:
    return timegm(datetime.utcnow().utctimetuple())


def get_signed_data(request: web.Request, key: str, require_exp: Optional[bool] = None) -> dict:
    token = request.rel_url.query.get("token")  # type: str

    if not token:
        CRISPY_LOGGER.debug("Client disconnected, no token")
        raise web.HTTPUnauthorized(text="No token provided")

    try:
        data = jwt.decode(token, key, algorithms=['HS256'])
    except (jwt.DecodeError, jwt.ExpiredSignatureError) as error:
        CRISPY_LOGGER.debug("Client disconnected, invalid token (%s)", error)
        raise web.HTTPBadRequest(text="Invalid token: {}".format(error))

    if require_exp and "exp" not in data:
        CRISPY_LOGGER.debug("Client disconnected, no exp provided")
        raise web.HTTPBadRequest(text="No exp provided")

    return data


async def listen_stream(request: web.Request) -> web.WebSocketResponse:
    CRISPY_LOGGER.debug("Client loop started")

    data = get_signed_data(request, LISTEN_SECRET, require_exp=True)

    websocket = web.WebSocketResponse()
    await websocket.prepare(request)

    stop_event = asyncio.Event()

    def client_loop_stop() -> None:
        stop_event.set()

    exp: int = data["exp"]
    now: int = get_utc_timestamp()

    if exp - now < 5:
        CRISPY_LOGGER.debug("Client disconnected, expiration too soon")
        raise web.HTTPBadRequest(text="Expiration too soon")

    asyncio.get_event_loop().call_later(exp - now, client_loop_stop)

    CRISPY_LOGGER.debug("Client loop started")
    with ClientQueue(data) as query:  # type: asyncio.Queue
        while not (stop_event.is_set() or SHUTDOWN.is_set()):
            event_poll = asyncio.Task(stop_event.wait())
            queue_get = asyncio.Task(query.get())
            shutdown_poll = asyncio.Task(SHUTDOWN.wait())

            try:
                done, pending = await asyncio.wait(
                    [shutdown_poll, event_poll, queue_get],
                    return_when=asyncio.FIRST_COMPLETED
                )  # type: Tuple[Set[asyncio.Future], Set[asyncio.Future]]
            except asyncio.CancelledError:
                shutdown_poll.cancel()
                event_poll.cancel()
                queue_get.cancel()
                CRISPY_LOGGER.debug("WebSocket closed by client")
                break

            for task in pending:
                task.cancel()
            completed_task = done.pop()

            try:
                result = completed_task.result()
            except RuntimeError:
                result = None

            if result is True:
                CRISPY_LOGGER.debug("WebSocket closed by global shutdown")
                break

            if websocket.closed:
                CRISPY_LOGGER.debug("WebSocket closed by client")
                break

            if result is None:
                CRISPY_LOGGER.debug("WebSocket closed by timeout")
                try:
                    await websocket.close()
                except RuntimeError:
                    pass
                except Exception:  # pylint: disable=broad-except
                    CRISPY_LOGGER.exception("Error closing client connection")
                break

            CRISPY_LOGGER.debug("Sending data to client")
            websocket.send_json(result)

    CRISPY_LOGGER.debug("Client loop finished")
    return websocket


async def send_message(request: web.Request) -> web.Response:
    props = get_signed_data(request, SEND_SECRET)
    data = await request.json()

    clients = match_client_queue(props)

    if clients:
        tasks = []

        for client in clients:
            tasks.append(asyncio.Task(client.put(data)))

        await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)

    return web.json_response({
        "completed": True,
        "clients": len(clients)
    })


async def on_shutdown(_: web.Application):
    SHUTDOWN.set()


application = web.Application()  # pylint: disable=invalid-name
application.on_shutdown.append(on_shutdown)
application.router.add_post('/message', send_message)
application.router.add_get('/message', listen_stream)


def run() -> None:
    logging.basicConfig(level=logging.DEBUG)
    web.run_app(application)


if __name__ == "__main__":
    run()
