#!env/bin/python3

import argparse
import asyncio
import json
import logging
import time
from datetime import datetime
import typing
from typing import Optional, Set, Tuple, NamedTuple  # pylint: disable=unused-import

import jwt
from aiohttp import web
from aiohttp.helpers import AccessLogger
import yaml

from crispywaffle.client import ClientQueue, match_client
from crispywaffle.apns import APNS_Client

CRISPY_LOGGER = logging.getLogger("crispy")


class BaseConfig:
    pass


class ServerConfig(BaseConfig):
    host: str = '0.0.0.0'
    port: int = 8080
    access_log_format: str = AccessLogger.LOG_FORMAT


class WSConfig(BaseConfig):
    ping_delay: int = 10
    secret: str


class APNSConfig(BaseConfig):
    key_id: str
    key_issuer: str
    key_data: str
    topic: str


class Config(BaseConfig):
    loglevel: int = logging.INFO
    logformat: str = logging.BASIC_FORMAT

    send_secret: str
    server: ServerConfig
    ws: WSConfig
    apns: APNSConfig


def load_config(config, data):
    fields = typing.get_type_hints(config)
    for field, type_ in fields.items():
        if field in data:
            value = data[field]
            if issubclass(type_, BaseConfig):
                value = load_config(type_, data[field])
            elif not isinstance(value, type_):
                value = type_(value)
            setattr(config, field, value)
        elif issubclass(type_, BaseConfig):
            setattr(config, field, type_())
    return config


def validate_require(config):
    fields = typing.get_type_hints(config)
    for field, type_ in fields.items():
        if not hasattr(config, field):
            raise ValueError(f'Field {field} from {config} is required')
        value = getattr(config, field)
        if issubclass(type_, BaseConfig):
            validate_require(value)
        elif not isinstance(value, type_):
            raise TypeError(
                f'Field {field} from {config} have incorrect type.'
                f'({type(value)} instead of {type_})'
            )


def get_signed_data(
        request: web.Request,
        key: str,
        require_exp: Optional[bool] = None
    ) -> dict:
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


class Message(NamedTuple):
    payload: dict
    filters: dict


class Client:
    def __init__(self, filters: dict, channels: list = None):
        self.filters = filters
        if channels is not None:
            self.channels = channels
        else:
            self.channels = []

    def match(self, filters) -> bool:
        for key, value in filters.items():
            if key not in self.filters or self.filters[key] != value:
                return False
        return True

    def send_message(self, message):
        for channel in self.channels:
            channel.send(message)


class ClientPool:
    def __init__(self):
        self.unique = {}
        self.anonimous = {}

    def update_user(self, channel, filters):
        if channel.uid is None:
            self.anonimous[channel] = Client(filters, [channel])
        else:
            if channel.uid in self.unique:
                client = self.unique[channel.uid]
                client.filters = filters
            else:
                self.unique[channel.uid] = client = Client(filters)
            client.channels.append(channel)

    def channel_gone(self, channel):
        if channel.uid is None:
            del self.anonimous[channel]
        else:
            client = self.unique[channel.uid]
            client.channels.remove(channel)
            if not client.channels:
                del self.unique[channel.uid]

    def route_message(self, message):
        for client in self.unique.values():
            if client.match(message.filters):
                client.send_message(message)

        for client in self.anonimous.values():
            if client.match(message.filters):
                client.send_message(message)


class WSChannel:
    def __init__(self, uid, ws, close_timeout):
        self.uid = uid
        self.ws = ws
        self._queue = asyncio.Queue()
        self._close = asyncio.Event()
        self._close_task = asyncio.get_event_loop().call_later(
            close_timeout, self._close.set
        )

    async def handle(self):
        CRISPY_LOGGER.debug('Client loop started')
        await self.ws.send_json({'test': 1})
        receive = asyncio.ensure_future(self.ws.receive())
        get_task = asyncio.ensure_future(self._queue.get())
        closed = asyncio.ensure_future(self._close.wait())
        while True:
            done, pending = await asyncio.wait(
                [receive, get_task, closed],
                return_when=asyncio.FIRST_COMPLETED,
            )
            done = done.pop()
            if done is receive:
                CRISPY_LOGGER.debug('Get data: %s', receive.result())
                receive = asyncio.ensure_future(self.ws.receive())
            elif done is get_task:
                message = get_task.result()
                await self.ws.send_json(message.payload)
                get_task = asyncio.ensure_future(self._queue.get())
            elif done is closed:
                await self.ws.close()
                break

    def close(self):
        if self._close.is_set():
            return
        self._close_task.cancel()
        self._close.set()

    def send(self, message):
        self._queue.put_nowait(message)

    def client_gone(self):
        self.close()


class WSProvider:
    def __init__(self, config, client_pool):
        self.config = config
        self.client_pool = client_pool
        self._sockets = []

    async def ws_connect(self, request: web.Request):
        CRISPY_LOGGER.debug('Connect request')
        data = get_signed_data(
            request,
            self.config.secret,
            require_exp=True
        )
        ws = web.WebSocketResponse(heartbeat=self.config.ping_delay)
        if not ws.can_prepare(request).ok:
            raise web.HTTPBadRequest(text='Can not prepare websocket')
        await ws.prepare(request)

        exp = data['exp']
        now = time.time()

        filters = data.get('fil')
        if not isinstance(filters, dict):
            CRISPY_LOGGER.debug("Client disconnected, invalid filters")
            raise web.HTTPBadRequest(text="Invalid filters")

        uid = data.get('uid')
        channel = WSChannel(uid, ws, exp-now)
        self._sockets.append(channel)
        self.client_pool.update_user(channel, filters)
        try:
            await channel.handle()
        except asyncio.CancelledError:
            CRISPY_LOGGER.debug('Cancelled')
            self.client_pool.channel_gone(channel)
        finally:
            self._sockets.remove(channel)

        if not ws.closed:
            CRISPY_LOGGER.debug('Closed')
            channel.close()
        return ws

    async def shutdown(self):
        for socket in self._sockets:
            socket.close()


async def send_message(request: web.Request) -> web.Response:
    data = get_signed_data(request, request.app['config'].send_secret)

    signed_filters: dict = data.get('fil')
    if signed_filters and not isinstance(signed_filters, dict):
        raise web.HTTPBadRequest(text="Invalid signed filters")
    else:
        signed_filters = {}

    try:
        payload = await request.json()
        assert isinstance(payload, dict)
    except (json.JSONDecodeError, AssertionError) as exc:
        CRISPY_LOGGER.debug("Message rejected, %s", exc)
        raise web.HTTPBadRequest(text=f'Invalid message body: {exc}')

    CRISPY_LOGGER.debug('Received message: %s', payload)

    if 'val' not in payload:
        raise web.HTTPBadRequest(text="No message value provided")

    custom_filters: dict = payload.get("fil") or {}
    if custom_filters and not isinstance(custom_filters, dict):
        raise web.HTTPBadRequest(text="Invalid custom filters")

    custom_filters.update(signed_filters)
    message = Message(payload['val'], custom_filters)

    request.app['clients'].route_message(message)
    return web.json_response({"queued": True})


async def apns_add_token(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except:
        raise web.HTTPBadRequest(text=json.dumps({'message': 'Invalid request'}))
    if data.get('auth') != '4864c003-fdad-49ce-b00e-8d1a2d2774fd':
        raise web.HTTPUnauthorized(text=json.dumps({'message': 'Bad authorization'}))

    try:
        request.app.apns_tokens[data['user']] = data['token']
    except KeyError:
        raise web.HTTPBadRequest(text=json.dumps({'message': 'Invalid request'}))

    return web.json_response({})


async def send_push(client, token, message, timeout):
    if timeout:
        await asyncio.sleep(timeout)
    await client.send_message(token, message)


async def apns_test_push(request: web.Request) -> web.Response:
    try:
        data = await request.json()
    except:
        raise web.HTTPBadRequest(text=json.dumps({'message': 'Invalid request'}))
    if data.get('auth') != '4864c003-fdad-49ce-b00e-8d1a2d2774fd':
        raise web.HTTPUnauthorized(text=json.dumps({'message': 'Bad authorization'}))
    try:
        user = data['user']
        message = data['message']
    except KeyError:
        raise web.HTTPBadRequest(text=json.dumps({'message': 'Invalid request'}))
    try:
        token = request.app.apns_tokens[user]
    except KeyError:
        raise web.HTTPBadRequest(text=json.dumps({'message': 'Missing token'}))
    try:
        timeout = float(data['timeout'])
        assert 0 <= timeout <= 30
    except (TypeError, KeyError, AssertionError):
        timeout = None

    asyncio.ensure_future(send_push(
        request.app.apn_client,
        token,
        message,
        timeout
    ))

    return web.json_response({'status': 'ok', 'timeout': timeout})


async def on_shutdown(app: web.Application):
    CRISPY_LOGGER.debug('Do shutdown')
    await app['ws'].shutdown()


def _load_config():
    parser = argparse.ArgumentParser("Message queue with JWT authentication")
    parser.add_argument(
        "--config", required=True, type=argparse.FileType('rb')
    )
    parser.add_argument(
        "--debug", action="store_const", dest="loglevel", const=logging.DEBUG
    )
    args = parser.parse_args()

    config = load_config(Config(), yaml.safe_load(args.config))
    if args.loglevel is not None:
        config.loglevel = args.loglevel
    validate_require(config)
    return config


def run_server() -> None:
    config = _load_config()

    logging.basicConfig(
        level=config.loglevel,
        format=config.logformat
    )

    application = web.Application()
    application.on_shutdown.append(on_shutdown)

    application['config'] = config
    application['clients'] = client_pool = ClientPool()
    application['ws'] = ws_provider = WSProvider(config.ws, client_pool)

    application.router.add_get('/message', ws_provider.ws_connect)
    application.router.add_post('/message', send_message)

    web.run_app(
        application, **vars(config.server)
    )


if __name__ == "__main__":
    run_server()
