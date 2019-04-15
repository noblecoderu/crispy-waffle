import json

from aiohttp import web

from crispywaffle.models import Message

from .functions import get_signed_data
from .logger import CRISPY_LOGGER


async def remove_user(request: web.Request) -> web.Response:
    get_signed_data(request, request.app["send_secret"])
    uid = request.match_info['uid']
    try:
        await request.app['clients'].remove_user(uid)
    except KeyError:
        return web.json_response({'message': 'no user'}, status=410)
    return web.json_response({'status': 'ok'})


async def send_message(request: web.Request) -> web.Response:
    request.app["stats"].push_message_received()
    data = get_signed_data(request, request.app["send_secret"])

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

    request.app['clients'].route_message(
        message=message,
        stats=request.app["stats"])
    return web.json_response({"queued": True})


async def short_user_info(request: web.Request) -> web.Response:
    get_signed_data(request, request.app["send_secret"])
    return web.json_response({
        uid: len(client.channels) for uid, client
        in request.app['clients'].unique.items()
    })
