import json

from aiohttp import web

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

    request.app['clients'].route_message(
        message=message,
        stats=request.app["stats"])
    return web.json_response({"queued": True})


@signed_data
async def short_user_info(request: web.Request) -> web.Response:
    match_params = dict(request.query)
    match_params.pop("token", None)

    return web.json_response({
        uid: len(client.channels) for uid, client
        in request.app['clients'].match_clients(match_params)
    })


@signed_data
async def match_user(request: web.Request) -> web.Response:
    data: dict = request["signed_data"]

    signed_filters: dict = data.get('fil')
    if signed_filters and not isinstance(signed_filters, dict):
        return web.json_response(
            {'message': "Invalid signed filters"}, status=400
        )
    else:
        signed_filters = {}

    try:
        payload = await request.json()
        assert isinstance(payload, dict)
    except (json.JSONDecodeError, AssertionError) as exc:
        CRISPY_LOGGER.debug("Message rejected, %s", exc)
        return web.json_response(
            {'message': f'Invalid message body: {exc}'}, status=400
        )

    CRISPY_LOGGER.debug('Received match request: %s', payload)

    custom_filters: dict = payload.get("fil") or {}
    if custom_filters and not isinstance(custom_filters, dict):
        return web.json_response(
            {'message': "Invalid custom filters"}, status=400
        )

    custom_filters.update(signed_filters)
    clients = request.app['clients'].match_clients(custom_filters)

    full_response = payload.get('opt', {}).get('full', False)
    if full_response:
        return web.json_response([client.filters for client in clients])
    else:
        return web.json_response({"matched": len(clients)})
