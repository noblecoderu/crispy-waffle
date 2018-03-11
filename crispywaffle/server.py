#!env/bin/python3
import argparse
import asyncio
import json
import logging
import time
import typing
from typing import Optional, NamedTuple
import uuid

import aiohttp
from aiohttp import web, helpers
import aioh2
import jwt
import yaml


CRISPY_LOGGER = logging.getLogger("crispy")


WS_CLOSE_TYPES = {
    aiohttp.WSMsgType.CLOSE,
    aiohttp.WSMsgType.CLOSING,
    aiohttp.WSMsgType.CLOSED
}


class BaseConfig:
    pass


class ServerConfig(BaseConfig):
    host: str = '0.0.0.0'
    port: int = 8080
    access_log_format: str = helpers.AccessLogger.LOG_FORMAT


class WSConfig(BaseConfig):
    ping_delay: int = 10
    secret: str


class APNSConfig(BaseConfig):
    key_id: str
    key_issuer: str
    key_data: str
    topic: str
    ttl: int = 60 * 60 * 24 * 30


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

    async def disconnect(self):
        futures = []
        for channel in self.channels:
            futures.append(channel.close())
        await asyncio.wait(futures)


class ClientPool:
    def __init__(self):
        self.unique = {}
        self.anonimous = {}

    def update_user(self, channel, filters):
        CRISPY_LOGGER.debug('Update user with uid %s', channel.uid)
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
        CRISPY_LOGGER.debug('Remove channel from user with uid %s', channel.uid)
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
        self.need_unregister = True

        self._close_timeout = close_timeout
        self._queue = asyncio.Queue()
        self._stopped = asyncio.Event()
        self._close_task = None
        self._read_task = None
        self._ws_read = None

    def __enter__(self):
        self._close_task = asyncio.ensure_future(self._close_coro())
        self._read_task = asyncio.ensure_future(self._queue.get())
        self._ws_read = asyncio.ensure_future(self.ws.receive())

    def __exit__(self, exception_type, exception_value, traceback):
        self._close_task.cancel()
        self._stopped.set()
        self._read_task.cancel()
        self._ws_read.cancel()
        if exception_type in (RuntimeError, asyncio.CancelledError):
            return True
        return None

    async def _close_coro(self):
        await asyncio.sleep(self._close_timeout)
        await self.close(wait=False)

    async def handle(self):
        CRISPY_LOGGER.debug('Client loop started')
        while True:
            done, _ = await asyncio.wait(
                [self._read_task, self._ws_read],
                return_when=asyncio.FIRST_COMPLETED,
            )
            done = done.pop()
            if done is self._read_task:
                message = done.result()
                await self.ws.send_json(message.payload)
                self._read_task = asyncio.ensure_future(self._queue.get())
            elif done is self._ws_read:
                message = done.result()
                if message.type in WS_CLOSE_TYPES:
                    break
                CRISPY_LOGGER.debug(
                    'Somewhy get data from websocket: %s', message.data
                )
                self._ws_read = asyncio.ensure_future(self.ws.receive())

    async def close(self, wait=True):
        self.need_unregister = False
        if not self.ws.closed:
            await self.ws.close()
        if wait:
            await self._stopped.wait()

    def send(self, message):
        self._queue.put_nowait(message)


class WSProvider:
    def __init__(self, config, client_pool):
        self.config = config
        self.client_pool = client_pool
        self._channels = []

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
        self._channels.append(channel)
        self.client_pool.update_user(channel, filters)
        with channel:
            await channel.handle()
        self._channels.remove(channel)
        if channel.need_unregister:
            self.client_pool.channel_gone(channel)

        return ws

    async def shutdown(self):
        if self._channels:
            await asyncio.wait([channel.close() for channel in self._channels])


class TokenInfo:
    def __init__(self, *channels):
        self.channels = set(channels)
        self.ttl_watcher = None


class APNSChannel:
    def __init__(self, uid, token, provider):
        self.uid = uid
        self.token = token
        self.provider = provider

        self._closed = asyncio.Event()

    async def close(self, wait=True):
        await self.provider.close_channel(self)

    def send(self, message):
        self.provider.add_message(self.token, message)


class APNSProvider:
    def __init__(self, config, client_pool):
        self.config = config
        self.client_pool = client_pool

        self._connection = None
        self._provider_token = None
        self._provider_token_iat = 0

        self._tokens = {}
        self._want_stop = asyncio.Event()
        self._stopped = asyncio.Event()
        self._queue = asyncio.Queue()
        self._loop_task = asyncio.ensure_future(self._queue_loop)

    async def add_token(self, user_id: str, token: str, filters):
        if token in self._tokens:
            info = self._tokens[token]
            info.ttl_watcher.cancel()
        else:
            info = self._tokens[token] = TokenInfo()
        channel = APNSChannel(user_id, token, self)
        info.channels.add(channel)
        self.client_pool.update_user(channel, filters)
        info.ttl_watcher = asyncio.get_event_loop().call_later(
            self.config.ttl, self.ttl_expire, token
        )

    def add_message(self, token: str, message):
        self._queue.put_nowait((token, message))

    def ttl_expire(self, token):
        channels = self._tokens.pop(token)
        for channel in channels:
            self.client_pool.channel_done(channel)

    async def _queue_loop(self):
        got_msg = asyncio.ensure_future(self._queue.get())
        want_stop = asyncio.ensure_future(self._want_stop.wait())
        while not self._want_stop.is_set() and not self._queue.empty():
            done, _ = asyncio.wait(
                [want_stop, got_msg],
                return_when=asyncio.FIRST_COMPLETED
            )
            done = done.pop()
            if done is got_msg:
                token, message = done.result()
                await self.add_message(token, message)
                self._queue.task_done()
                got_msg = asyncio.ensure_future(self._queue.get())
            elif done is want_stop:
                # Create dummy future for asyncio.wait compatibility
                want_stop = asyncio.Future()

        want_stop.cancel()
        got_msg.cancel()
        self._stopped.set()

    async def close_channel(self, channel):
        info = self._tokens[channel.token]
        info.channels.remove(channel)
        if not info.channels:
            info.ttl_watcher.cancel()
            del self._tokens[channel.token]

    def get_provider_token(self):
        now = time.time()
        # Token live time - one hour
        if (now - self._provider_token_iat) > 3600:
            self._provider_token_iat = now
            self._provider_token = jwt.encode(
                {
                    'iss': self.config.key_issuer,
                    'iat': round(now)
                },
                self.config.key_data,
                headers={'kid': self.config.key_id},
                algorithm='ES256'
            ).decode()
        return self._provider_token()

    async def setup_connection(self):
        self._connection = await aioh2.open_connection(
            'api.push.apple.com',
            port=443,
            ssl=True,
            functional_timeout=0.1
        )

    async def send_message(self, token: str, message: Message):
        if self._connection is None or not self._connection._conn:
            await self.setup_connection()

        apns_id = str(uuid.uuid4())
        stream_id = await self._connection.start_request([
            (':method', 'POST'),
            (':path', f'/3/device/{token}'),
            (':scheme', 'https'),
            ('host', 'api.push.apple.com'),
            ('authorization', f'bearer {self.get_provider_token()}'),
            ('apns-id', apns_id),
            ('apns-topic', self.config.topic)
        ])

        # TODO: Make valid paylod
        await self._connection.send_data(
            stream_id,
            json.dumps({'aps': {'alert': 'HELLO'}}).encode(),
            end_stream=True
        )

        # TODO: do response checks
        headers = await self._connection.recv_response(stream_id)
        print('Response headers:', headers)
        resp = await self._connection.read_stream(stream_id, -1)
        print('Response body:', resp)
        trailers = await self._connection.recv_trailers(stream_id)
        print('Response trailers:', trailers)

        if False:
            info = self._tokens.pop(token)
            for channel in info.channels:
                self.client_pool.channel_gone(channel)

    async def shutdown(self):
        self._want_stop.set()
        await self._stopped.wait()


async def apns_add_token(request: web.Request) -> web.Response:
    data = get_signed_data(request, request, request.app['config'].send_secret)
    try:
        uid = data['uid']
        token = data['token']
        filters = data['filters']
    except KeyError:
        raise web.HTTPBadRequest(
            text=json.dumps({'message': 'Invalid request'})
        )
    await request.app['apns'].add_token(uid, token, filters)
    return web.json_response({'status': 'ok'})


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


async def on_shutdown(app: web.Application):
    CRISPY_LOGGER.debug('Do shutdown')
    await asyncio.gather(app['ws'].shutdown(), app['apns'].shutdown())


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
    application['apns'] = APNSProvider(config.apns, client_pool)

    application.router.add_get('/message', ws_provider.ws_connect)
    application.router.add_post('/message', send_message)
    application.router.add_post('/apns/add_token', apns_add_token)

    web.run_app(
        application, **vars(config.server)
    )


if __name__ == "__main__":
    run_server()
