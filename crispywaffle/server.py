#!env/bin/python3
import argparse
import asyncio
import contextlib
import json
import logging
import time
import typing
from typing import Optional, NamedTuple, Dict
import uuid

import aiohttp
import aioredis
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
    redis: str = ''


class Config(BaseConfig):
    loglevel: int = logging.INFO
    logformat: str = logging.BASIC_FORMAT

    send_secret: str
    server: ServerConfig
    ws: WSConfig
    apns: APNSConfig


def load_config(config: BaseConfig, data: dict) -> BaseConfig:
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


def validate_require(config: BaseConfig):
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
    def __init__(self, filters: Dict[str, str], channels: list = None):
        self.filters = filters
        if channels is not None:
            self.channels = channels
        else:
            self.channels = []

    def match(self, filters: Dict[str, str]) -> bool:
        for key, value in filters.items():
            if key not in self.filters or self.filters[key] != value:
                return False
        return True

    def send_message(self, message: Message):
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

    def update_user(self, channel, filters: Dict[str, str]) -> None:
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
        CRISPY_LOGGER.debug(
            'Remove channel from user with uid %s', channel.uid
        )
        if channel.uid is None:
            del self.anonimous[channel]
        else:
            client = self.unique[channel.uid]
            client.channels.remove(channel)
            if not client.channels:
                del self.unique[channel.uid]

    def route_message(self, message: Message):
        for client in self.unique.values():
            if client.match(message.filters):
                client.send_message(message)

        for client in self.anonimous.values():
            if client.match(message.filters):
                client.send_message(message)

    async def remove_user(self, uid):
        client = self.unique.pop(uid)
        await client.disconnect()


class WSChannel:
    def __init__(
            self,
            uid: str,
            ws: web.WebSocketResponse,
            close_timeout: int
        ) -> None:
        self.uid = uid
        self.ws = ws
        self.notify_pool = True

        self._close_timeout = close_timeout
        self._queue = asyncio.Queue()
        self._want_stop = asyncio.Event()
        self._stopped = asyncio.Event()
        self._want_stop_event = asyncio.Event()
        self._ttl_expire_task = None
        self._want_stop = None
        self._read_task = None
        self._ws_read = None

    def __enter__(self):
        self._ttl_expire_task = asyncio.ensure_future(self._ttl_expire())
        self._read_task = asyncio.ensure_future(self._queue.get())
        self._ws_read = asyncio.ensure_future(self.ws.receive())
        self._want_stop = asyncio.ensure_future(self._want_stop_event.wait())

    def __exit__(self, exception_type, exception_value, traceback):
        self._ttl_expire_task.cancel()
        self._stopped.set()
        self._read_task.cancel()
        self._ws_read.cancel()
        self._want_stop.cancel()
        CRISPY_LOGGER.debug(
            'Disconnect websocket queue size: %s', self._queue.qsize()
        )
        return exception_type in (RuntimeError, asyncio.CancelledError)

    async def _ttl_expire(self):
        await asyncio.sleep(self._close_timeout)
        await self.ws.close()

    async def handle(self):
        CRISPY_LOGGER.debug('Client loop started')
        while not self._want_stop_event.is_set() or not self._queue.empty():
            done, _ = await asyncio.wait(
                [self._read_task, self._ws_read, self._want_stop],
                return_when=asyncio.FIRST_COMPLETED,
            )
            done = done.pop()
            if done is self._read_task:
                message = done.result()
                await self.ws.send_json(message.payload)
                self._queue.task_done()
                self._read_task = asyncio.ensure_future(self._queue.get())
            elif done is self._ws_read:
                message = done.result()
                if message.type in WS_CLOSE_TYPES:
                    break
                CRISPY_LOGGER.debug(
                    'Somewhy get data from websocket: %s', message.data
                )
                self._ws_read = asyncio.ensure_future(self.ws.receive())
            elif done is self._want_stop:
                self._want_stop = asyncio.Future()

    async def close(self):
        self.notify_pool = False
        if not self.ws.closed:
            await self.ws.close()
        await self._stopped.wait()

    def send(self, message):
        self._queue.put_nowait(message)


class WSProvider:
    def __init__(self, config: WSConfig, client_pool: ClientPool):
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
        if channel.notify_pool:
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
    def __init__(self, uid: str, token: str, provider: 'APNSProvider'):
        self.uid = uid
        self.token = token
        self.provider = provider

    def send(self, message: Message):
        self.provider.queue_message(self.token, message)

    async def close(self):
        await self.provider.close_channel(self)


class APNSProvider:
    def __init__(self, config: APNSConfig, client_pool: ClientPool):
        self.config = config
        self.client_pool = client_pool

        self._connection = None
        self._provider_token = None
        self._provider_token_iat = 0
        self._redis = None

        self._tokens = {}
        self._want_stop = asyncio.Event()
        self._stopped = asyncio.Event()
        self._queue = asyncio.Queue()
        self._loop_task = asyncio.ensure_future(self._queue_loop)

    async def redis_load(self):
        self._redis = await aioredis.create_redis(self.config.redis)
        cursor = b'0'
        async for token in self._redis.iscan():
            channels = [
                APNSChannel(uid, token, self) async for uid in
                self._redis.isscan(token)
            ]
            ttl = await self._redis.ttl(token)
            info = TokenInfo(*channels)
            info.ttl_watcher = asyncio.get_event_loop().call_later(
                ttl, self.ttl_expire, token
            )
            for cahnnel in channels:
                # TODO: load filters from somewhere
                self.client_pool.update_user(channel, {})

    def add_token(
            self,
            user_id: str,
            token: str,
            filters: Dict[str, str]
        ) -> None:
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

    def remove_token(self, token: str, uid: Optional[str] = None):
        info = self._tokens.pop(token)
        if uid is not None:
            for channel in info.channels:
                if channel.uid == uid:
                    break
            else:
                return
            info.channels.remove(channel)
            self.client_pool.channel_gone(channel)
            if info.channels:
                self._tokens[token] = info
        else:
            for channel in info.channels:
                self.client_pool.channel_gone(channel)

    def queue_message(self, token: str, message):
        self._queue.put_nowait((token, message))

    def ttl_expire(self, token):
        channels = self._tokens.pop(token)
        for channel in channels:
            self.client_pool.channel_done(channel)

    async def _queue_loop(self):
        got_msg = asyncio.ensure_future(self._queue.get())
        want_stop = asyncio.ensure_future(self._want_stop.wait())
        while not self._want_stop.is_set() or not self._queue.empty():
            done, _ = asyncio.wait(
                [want_stop, got_msg],
                return_when=asyncio.FIRST_COMPLETED
            )
            done = done.pop()
            if done is got_msg:
                token, message = done.result()
                await self.send_message(token, message)
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
            with contextlib.suppress(KeyError):
                self.remove_token(token)

    async def shutdown(self):
        self._want_stop.set()
        await self._stopped.wait()


async def apns_add_token(request: web.Request) -> web.Response:
    get_signed_data(request, request.app['config'].send_secret)
    try:
        data = await request.json()
        uid = data['uid']
        token = data['token']
        filters = data['filters']
    except KeyError:
        raise web.HTTPBadRequest(
            text=json.dumps({'message': 'Invalid request'})
        )
    request.app['apns'].add_token(uid, token, filters)
    return web.json_response({'status': 'ok'})


async def apns_remove_token(request: web.Request) -> web.Response:
    get_signed_data(request, request.app['config'].send_secret)
    try:
        token = request.match_info['token']
        assert token
    except (KeyError, AssertionError):
        raise web.HTTPBadRequest(
            text=json.dumps({'message': 'Invalid request'})
        )
    try:
        data = await request.json()
    except:
        data = {}
    try:
        request.app['apns'].remove_token(token, data.get('uid'))
    except KeyError:
        return web.json_response({'error': 'no token'})
    return web.json_response({'status': 'ok'})


async def remove_user(request: web.Request) -> web.Response:
    get_signed_data(request, request.app['config'].send_secret)
    uid = request.match_info['uid']
    try:
        await request.app['clients'].remove_user(uid)
    except KeyError:
        return web.json_response({'error': 'no user'})
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
    application.router.add_post('/apns/token', apns_add_token)
    application.router.add_delete('/apns/token/{token}', apns_remove_token)
    application.router.add_delete('/user/{uid}', remove_user)

    web.run_app(application, **vars(config.server))


if __name__ == "__main__":
    run_server()
