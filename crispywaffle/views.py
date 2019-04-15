import asyncio
import json
from calendar import timegm
from datetime import datetime

from aiohttp import web

from crispywaffle.client import Client
from crispywaffle.models import Message

from .functions import get_signed_data, signed_data
from .logger import CRISPY_LOGGER


@signed_data
async def remove_user(request: web.Request) -> web.Response:
    uid = request.match_info['uid']
    try:
        await request.app['clients'].remove_user(uid)
    except KeyError:
        return web.json_response({'message': 'no user'}, status=410)
    return web.json_response({'status': 'ok'})


async def listen_stream(request: web.Request) -> web.WebSocketResponse:
    data = get_signed_data(request, request.app["listen_secret"], require_exp=True)

    websocket = web.WebSocketResponse()
    await websocket.prepare(request)

    exp: int = data["exp"]
    now: int = timegm(datetime.utcnow().utctimetuple())

    if exp - now < 5:
        CRISPY_LOGGER.debug("Client disconnected, expiration too soon")
        raise web.HTTPBadRequest(text="Expiration too soon")

    filters = data.get("fil")
    if not isinstance(filters, dict):
        CRISPY_LOGGER.debug("Client disconnected, invalid filters")
        raise web.HTTPBadRequest(text="Invalid filters")

    signature_timeout = asyncio.Task(asyncio.sleep(exp - now))

    CRISPY_LOGGER.debug("Client loop started")
    with Client(filters, request.app["clients"]) as client:
        while not websocket.closed:
            queue_get = asyncio.Task(client.queue.get())
            ping_sleep = asyncio.Task(asyncio.sleep(request.app["ping_delay"]))

            try:
                done, pending = await asyncio.wait(
                    [ping_sleep, queue_get, signature_timeout],
                    return_when=asyncio.FIRST_COMPLETED
                )
                completed_task: asyncio.Task = done.pop()
            except asyncio.CancelledError:
                ping_sleep.cancel()
                queue_get.cancel()
                CRISPY_LOGGER.debug("WebSocket closed by client")
                break

            if completed_task is ping_sleep:
                await websocket.ping()
                await websocket.send_json({"hue": "hue"})
                queue_get.cancel()
            elif completed_task is queue_get:
                data = queue_get.result()
                await websocket.send_json(data)
                CRISPY_LOGGER.debug("Sending data to client")
                ping_sleep.cancel()
            else:
                ping_sleep.cancel()
                queue_get.cancel()
                break

    signature_timeout.cancel()

    # noinspection PyBroadException
    try:
        await websocket.close()
    except RuntimeError:
        pass
    except Exception:  # pylint: disable=broad-except
        CRISPY_LOGGER.exception("Error closing client connection")

    CRISPY_LOGGER.debug("Client loop finished")
    return websocket


@signed_data
async def send_message(request: web.Request) -> web.Response:
    request.app["stats"].push_message_received()
    data = request["signed_data"]

    signed_filters: dict = data.get('fil')
    if signed_filters and not isinstance(signed_filters, dict):
        return web.json_response(
            {'message': "Invalid signed filters"}, status=400
        )

    if not signed_filters:
        signed_filters = {}

    try:
        payload = await request.json()
        assert isinstance(payload, dict)
    except (json.JSONDecodeError, AssertionError) as exc:
        CRISPY_LOGGER.debug("Message rejected, %s", exc)
        return web.json_response(
            {'message': f'Invalid message body: {exc}'}, status=400
        )

    CRISPY_LOGGER.debug('Received message: %s', payload)

    if 'val' not in payload:
        return web.json_response(
            {"message": "No message value provided"}, status=400
        )

    custom_filters: dict = payload.get("fil") or {}
    if custom_filters and not isinstance(custom_filters, dict):
        return web.json_response(
            {'message': "Invalid custom filters"}, status=400
        )

    custom_filters.update(signed_filters)
    message = Message(payload['val'], custom_filters)

    request.app["clients"].put_message(message)
    return web.json_response({"queued": True})


@signed_data
async def short_user_info(request: web.Request) -> web.Response:
    match_params = dict(request.query)
    match_params.pop("token", None)

    matched = [
        client.filters for client
        in request.app['clients'].matching_clients_iter(match_params)
    ]

    if request.method == 'HEAD':
        return web.json_response(headers={'X-Count': str(len(matched))})
    if request.method == 'GET':
        return web.json_response(
            matched,
            headers={'X-Count': str(len(matched))}
        )
