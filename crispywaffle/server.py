import asyncio
import json
import time
from typing import TYPE_CHECKING, Dict, Optional

import aiohttp
from aiohttp import web

from .functions import get_signed_data
from .logger import CRISPY_LOGGER
from .models import Message

if TYPE_CHECKING:
    # pylint: disable=unused-import,ungrouped-imports
    from crispywaffle.stats import StatCollector


WS_CLOSE_TYPES = {
    aiohttp.WSMsgType.CLOSE,
    aiohttp.WSMsgType.CLOSING,
    aiohttp.WSMsgType.CLOSED
}


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

    def send_message(self, message: Message, stats: "StatCollector"):
        for channel in self.channels:
            channel.send(message)
            stats.push_message_sent()

    async def disconnect(self):
        futures = []
        for channel in self.channels:
            futures.append(channel.close())
        await asyncio.wait(futures)


class ClientPool:
    def __init__(self):
        self.unique = {}
        self.anonymous = {}
        self._redis = None

    def set_redis(self, redis):
        self._redis = redis

    async def update_user(
            self,
            uid: Optional[str],
            filters: Dict[str, str],
            *channels,
            ttl: Optional[int] = None) -> None:
        CRISPY_LOGGER.debug('Update user with uid %s', uid)
        if uid is None:
            if len(channels) != 1:
                raise RuntimeError(
                    'Channels should contain one element for anonymous client.'
                )
            self.anonymous[channels[0]] = Client(filters, list(channels))
        else:
            if uid in self.unique:
                client = self.unique[uid]
                client.filters = filters
            else:
                self.unique[uid] = client = Client(filters)
            client.channels.extend(channels)
            if ttl is not None and self._redis is not None:
                key = f'filters:{uid}'
                await self._redis.set(key, json.dumps(filters))
                await self._redis.expire(key, ttl)

    def add_channel(self, uid, *channels):
        client = self.unique[uid]
        client.channels.extend(channels)

    async def clean_clients(self):
        new_unique = {}
        to_remove = []
        for uid, client in self.unique.items():
            if client.channels:
                new_unique[uid] = client
            else:
                to_remove.append(uid)

        self.unique = new_unique
        if self._redis is not None and to_remove:
            await self._redis.unlink(*map('filters:{}'.format, to_remove))

    async def load_from_cache(self):
        async for key in self._redis.iscan(match='filters:*'):
            try:
                filters = json.loads(await self._redis.get(key))
            except json.JSONDecodeError:
                await self._redis.unlink(key)
                continue
            uid = key.split(':')[1]
            self.unique[uid] = Client(filters)

    async def channel_gone(self, channel):
        CRISPY_LOGGER.debug(
            'Remove channel from user with uid %s', channel.uid
        )
        if channel.uid is None:
            del self.anonymous[channel]
        else:
            client = self.unique[channel.uid]
            client.channels.remove(channel)
            if not client.channels:
                del self.unique[channel.uid]
                if self._redis is not None:
                    await self._redis.unlink(f'filters:{channel.uid}')

    def route_message(self, message: Message, stats: "StatCollector"):
        routed = False

        for client in self.unique.values():
            if client.match(message.filters):
                client.send_message(message, stats)
                routed = True

        for client in self.anonymous.values():
            if client.match(message.filters):
                client.send_message(message, stats)
                routed = True

        if routed:
            stats.push_message_routed()
        else:
            stats.push_message_missed()

    def match_clients(self, filters: dict) -> dict:
        clients = {}

        for uid, client in self.unique.items():
            if client.match(filters):
                clients[uid] = client

        for uid, client in self.anonymous.items():
            if client.match(filters):
                clients[uid] = client

        return clients

    async def remove_user(self, uid):
        client = self.unique.pop(uid)
        await client.disconnect()
        if self._redis is not None:
            await self._redis.delete(f'filters:{uid}')


class WSChannel:
    def __init__(
            self,
            uid: str,
            ws: web.WebSocketResponse,
            close_timeout: int) -> None:
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
            await asyncio.wait(
                [
                    self._read_task,
                    self._ws_read,
                    self._want_stop
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            if self._read_task.done():
                message = self._read_task.result()
                await self.ws.send_json(message.payload)
                self._queue.task_done()
                self._read_task = asyncio.ensure_future(self._queue.get())
            elif self._ws_read.done():
                message = self._ws_read.result()
                if message.type in WS_CLOSE_TYPES:
                    break
                CRISPY_LOGGER.debug(
                    'Somewhy got data from websocket: %s', message.data
                )
                self._ws_read = asyncio.ensure_future(self.ws.receive())
            elif self._want_stop.done():
                self._want_stop = asyncio.Future()

    async def close(self):
        self.notify_pool = False
        if not self.ws.closed:
            await self.ws.close()
        await self._stopped.wait()

    def send(self, message):
        self._queue.put_nowait(message)


class WSProvider:
    def __init__(self, secret: str, client_pool: ClientPool):
        self.secret = secret

        self.ping_delay: int = 10
        self.client_pool = client_pool
        self._channels = []

    async def ws_connect(self, request: web.Request):
        CRISPY_LOGGER.debug('Connect request')
        data = get_signed_data(
            request,
            self.secret,
            require_exp=True
        )
        ws_response = web.WebSocketResponse(heartbeat=self.ping_delay)
        if not ws_response.can_prepare(request).ok:
            raise web.HTTPBadRequest(text='Can not prepare websocket')
        await ws_response.prepare(request)

        exp = data['exp']
        now = time.time()

        filters = data.get('fil')
        if not isinstance(filters, dict):
            CRISPY_LOGGER.debug("Client disconnected, invalid filters")
            raise web.HTTPBadRequest(text="Invalid filters")

        uid = data.get('uid')
        channel = WSChannel(uid, ws_response, exp-now)

        self._channels.append(channel)
        await self.client_pool.update_user(uid, filters, channel)

        with channel:
            await channel.handle()

        self._channels.remove(channel)
        if channel.notify_pool:
            await self.client_pool.channel_gone(channel)

        return ws_response

    async def shutdown(self):
        if self._channels:
            await asyncio.wait([
                channel.close()
                for channel in self._channels
            ])
