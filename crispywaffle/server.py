import argparse
import asyncio
import json
import logging
from calendar import timegm
from datetime import datetime
from typing import Optional, Set, Tuple  # pylint: disable=unused-import

import jwt
from aiohttp import web

from crispywaffle.client import ClientQueue, match_client_queue

CRISPY_LOGGER = logging.getLogger("crispy")


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

    data = get_signed_data(request, request.app.listen_secret, require_exp=True)

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

    filters = data.get("fil")
    if not isinstance(filters, dict):
        CRISPY_LOGGER.debug("Client disconnected, invalid filters")
        raise web.HTTPBadRequest(text="Invalid filters")

    asyncio.get_event_loop().call_later(exp - now, client_loop_stop)

    CRISPY_LOGGER.debug("Client loop started")
    with ClientQueue(filters) as query:  # type: asyncio.Queue
        while not (stop_event.is_set() or request.app.shutdown_event.is_set()):
            event_poll = asyncio.Task(stop_event.wait())
            queue_get = asyncio.Task(query.get())
            shutdown_poll = asyncio.Task(request.app.shutdown_event.wait())

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
                if stop_event.is_set():
                    CRISPY_LOGGER.debug("WebSocket closed by session timeout")
                else:
                    CRISPY_LOGGER.debug("WebSocket closed by global shutdown")
                break

            if websocket.closed:
                CRISPY_LOGGER.debug("WebSocket closed by client")
                break

            if result is None:
                CRISPY_LOGGER.debug("WebSocket closed by timeout")
                # noinspection PyBroadException
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
    data = get_signed_data(request, request.app.send_secret)
    message = await request.json()

    filters = data.get("fil")
    if not isinstance(filters, dict):
        raise web.HTTPBadRequest(text="Invalid filters")

    clients = match_client_queue(filters)

    for client in clients:
        asyncio.ensure_future(client.put(message))

    return web.json_response({
        "queued": True,
        "clients": len(clients)
    })


async def on_shutdown(app: web.Application):
    app.shutdown_event.set()


application = web.Application()  # pylint: disable=invalid-name
application.on_shutdown.append(on_shutdown)
application.router.add_post('/message', send_message)
application.router.add_get('/message', listen_stream)

application.shutdown_event = asyncio.Event()


def run() -> None:
    parser = argparse.ArgumentParser("Message queue with JWT authentication")
    parser.add_argument("--listen", dest="listen_secret", required=True)
    parser.add_argument("--send", dest="send_secret", required=True)

    args = parser.parse_args()

    application.listen_secret = args.listen_secret
    application.send_secret = args.send_secret

    logging.basicConfig(level=logging.DEBUG)
    web.run_app(application)


def utils() -> None:
    parser = argparse.ArgumentParser("Message queue with JWT authentication")
    parser.add_argument("--secret", dest="secret", required=True)
    parser.add_argument("--sign", dest="message", required=True)

    args = parser.parse_args()
    data = json.loads(args.message)

    print(jwt.encode(data, args.secret.strip(), algorithm="HS256"))

if __name__ == "__main__":
    run()
