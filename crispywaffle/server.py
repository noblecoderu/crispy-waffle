#!env/bin/python3

import argparse
import asyncio
import logging
from calendar import timegm
from datetime import datetime
from typing import Optional, Set, Tuple  # pylint: disable=unused-import

import jwt
from aiohttp import web

from crispywaffle.client import ClientQueue, match_client

CRISPY_LOGGER = logging.getLogger("crispy")


def get_utc_timestamp() -> int:
    return timegm(datetime.utcnow().utctimetuple())


def get_signed_data(request: web.Request, key: str, require_exp: Optional[bool] = None) -> dict:
    token: str = request.rel_url.query.get("token")

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


async def ping_loop(websocket: web.WebSocketResponse, delay: int = 10) -> None:
    while not websocket.closed:
        CRISPY_LOGGER.debug("Sending ping to client")
        websocket.ping()
        await asyncio.sleep(delay)


async def listen_stream(request: web.Request) -> web.WebSocketResponse:
    data = get_signed_data(request, request.app.listen_secret, require_exp=True)

    websocket = web.WebSocketResponse()
    await websocket.prepare(request)

    stop_event = asyncio.Event()

    exp: int = data["exp"]
    now: int = get_utc_timestamp()

    if exp - now < 5:
        CRISPY_LOGGER.debug("Client disconnected, expiration too soon")
        raise web.HTTPBadRequest(text="Expiration too soon")

    filters = data.get("fil")
    if not isinstance(filters, dict):
        CRISPY_LOGGER.debug("Client disconnected, invalid filters")
        raise web.HTTPBadRequest(text="Invalid filters")

    stop_handle = asyncio.get_event_loop().call_later(exp - now, lambda: stop_event.set())
    asyncio.ensure_future(ping_loop(websocket, request.app.ping_delay))

    CRISPY_LOGGER.debug("Client loop started")
    with ClientQueue(filters) as query:
        while not (stop_event.is_set() or request.app.shutdown_event.is_set()):
            queue_get = asyncio.Task(query.get())

            stop_event_poll = asyncio.Task(stop_event.wait())
            shutdown_poll = asyncio.Task(request.app.shutdown_event.wait())

            try:
                done, pending = await asyncio.wait(
                    [shutdown_poll, stop_event_poll, queue_get],
                    return_when=asyncio.FIRST_COMPLETED
                )  # type: Tuple[Set[asyncio.Future], Set[asyncio.Future]]
            except asyncio.CancelledError:
                shutdown_poll.cancel()
                stop_event_poll.cancel()
                queue_get.cancel()
                CRISPY_LOGGER.debug("WebSocket closed by client")
                break

            for task in pending:
                task.cancel()
            completed_task = done.pop()

            try:
                result = completed_task.result()
            except RuntimeError:
                continue

            if result is True:
                if stop_event.is_set():
                    CRISPY_LOGGER.debug("WebSocket closed by session timeout")
                else:
                    CRISPY_LOGGER.debug("WebSocket closed by global shutdown")
                break

            if websocket.closed:
                CRISPY_LOGGER.debug("WebSocket closed by client")
                break

            CRISPY_LOGGER.debug("Sending data to client")
            websocket.send_json(result)

    # noinspection PyBroadException
    try:
        await websocket.close()
    except RuntimeError:
        pass
    except Exception:  # pylint: disable=broad-except
        CRISPY_LOGGER.exception("Error closing client connection")

    stop_handle.cancel()

    CRISPY_LOGGER.debug("Client loop finished")
    return websocket


async def send_message(request: web.Request) -> web.Response:
    data = get_signed_data(request, request.app.send_secret)

    signed_filters: dict = data.get("fil")
    if signed_filters and not isinstance(signed_filters, dict):
        raise web.HTTPBadRequest(text="Invalid signed filters")
    else:
        signed_filters = {}

    message = await request.json()
    if not isinstance(message, dict):
        raise web.HTTPBadRequest(text="Invalid message body")

    if "val" not in message:
        raise web.HTTPBadRequest(text="No message value provided")
    value = message["value"]

    custom_filters: dict = message.get("fil")
    if custom_filters and not isinstance(custom_filters, dict):
        raise web.HTTPBadRequest(text="Invalid custom filters")
    else:
        custom_filters = {}

    custom_filters.update(signed_filters)

    for client in match_client(custom_filters):
        client.put(value)

    return web.json_response({"queued": True})


async def on_shutdown(app: web.Application):
    app.shutdown_event.set()


application = web.Application()  # pylint: disable=invalid-name
application.on_shutdown.append(on_shutdown)
application.router.add_post('/message', send_message)
application.router.add_get('/message', listen_stream)

application.shutdown_event = asyncio.Event()


def run_server() -> None:
    parser = argparse.ArgumentParser("Message queue with JWT authentication")
    parser.add_argument("--listen", dest="listen_secret", required=True)
    parser.add_argument("--send", dest="send_secret", required=True)
    parser.add_argument("--ping-delay", dest="ping_delay", type=int, default=10)
    parser.add_argument("--host", dest="host", type=str, default="0.0.0.0")
    parser.add_argument("--port", dest="port", type=int, default=8080)

    args = parser.parse_args()

    application.listen_secret = args.listen_secret
    application.send_secret = args.send_secret
    application.ping_delay = args.ping_delay

    logging.basicConfig(level=logging.DEBUG)
    web.run_app(application, host=args.host, port=args.port)


if __name__ == "__main__":
    run_server()
