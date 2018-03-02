import functools
import json
import time
import uuid

import aioh2
from aiohttp import web
import jwt


class APN_Client:
    def __init__(self, config):
        self.config = config
        self.connection = None

        self._token = None
        self._token_iat = 0

    def get_token(self) -> str:
        now = time.time()
        if (now - self._token_iat) > 3600:
            self._token_iat = now
            self._token = jwt.encode(
                {
                    "iss": self.config.apns_key_issuer,
                    "iat": round(now)
                },
                self.config.apns_key_data,
                headers={"kid": self.config.apns_key_id},
                algorithm='ES256'
            ).decode()
        return self._token

    async def setup_connection(self):
        self.connection = await aioh2.open_connection(
            'api.push.apple.com',
            port=443,
            ssl=True,
            functional_timeout=0.1
        )

    async def send_message(self, token: str, message: str):
        if not self.connection or not self.connection._conn:
            await self.setup_connection()

        rtt = await self.connection.wait_functional()
        if rtt:
            print('Round-trip time: %.1fms' % (rtt * 1000))

        stream_id = await self.connection.start_request([
            (':method', 'POST'),
            (':path', f'/3/device/{token}'),
            (':scheme', 'https'),
            ('host', 'api.push.apple.com'),
            ('authorization', f'bearer {self.get_token()}'),
            ('apns-id', self.config.apns_id),
            ('apns-topic', self.config.apns_topic),
        ])

        await self.connection.send_data(
            stream_id,
            json.dumps({"aps": {"alert": message}}).encode(),
            end_stream=True
        )

        headers = await self.connection.recv_response(stream_id)
        print('Response headers:', headers)
        resp = await self.connection.read_stream(stream_id, -1)
        print('Response body:', resp)
        trailers = await self.connection.recv_trailers(stream_id)
        print('Response trailers:', trailers)
