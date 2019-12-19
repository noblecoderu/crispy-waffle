import asyncio
import calendar
import pickle
import struct
from datetime import datetime
from numbers import Number
from typing import List, Tuple


class StatCollector:

    def __init__(self) -> None:
        self.messages_received: int = 0
        self.messages_sent: int = 0

        self.messages_routed: int = 0
        self.messages_missed: int = 0

    def push_message_received(self) -> None:
        self.messages_received += 1

    def push_message_sent(self) -> None:
        self.messages_sent += 1

    def push_message_routed(self) -> None:
        self.messages_routed += 1

    def push_message_missed(self) -> None:
        self.messages_missed += 1


class GraphiteStatPusher:

    def __init__(self,
                 collector: StatCollector,
                 host: str,
                 stats_root: str = "",
                 frequency: int = 10,) -> None:
        self.collector = collector
        self.stats_root = stats_root

        self.host = host
        self.port: int = 2004
        self.sleep: int = frequency
        self.queue = asyncio.Queue(maxsize=100)

    async def send(self, data: bytes) -> None:
        for _ in range(3):
            try:
                connection = await asyncio.open_connection(self.host, self.port)
                _, writer = await asyncio.wait_for(connection, timeout=5)
                break
            except asyncio.TimeoutError:
                continue
            except ConnectionRefusedError:
                return
        else:
            return

        writer.write(data)
        await writer.drain()

        writer.close()

    def prefix_key(self, key: str) -> str:
        if self.stats_root:
            return f"{self.stats_root}.{key}"

        return key

    @staticmethod
    def pack_data(metric_list: List[Tuple[str, Tuple[int, Number]]]) -> bytes:
        payload = pickle.dumps(metric_list, protocol=2)
        header = struct.pack("!L", len(payload))
        message = header + payload

        return message

    def collect_metrics(self) -> List[Tuple[str, Tuple[int, Number]]]:
        metrics = []

        now = datetime.utcnow()
        timestamp: int = calendar.timegm(now.utctimetuple())

        metrics.append(
            (
                self.prefix_key("messages_received"),
                (timestamp, self.collector.messages_received)
            )
        )
        metrics.append(
            (
                self.prefix_key("messages_routed"),
                (timestamp, self.collector.messages_routed)
            )
        )
        metrics.append(
            (
                self.prefix_key("messages_missed"),
                (timestamp, self.collector.messages_missed)
            )
        )

        return metrics

    async def send_data(self) -> None:
        while True:
            data = await self.queue.get()
            await self.send(data)

    async def push_metrics(self) -> None:
        while True:
            await asyncio.sleep(self.sleep)

            metrics = self.collect_metrics()
            packed_data = self.pack_data(metrics)

            try:
                self.queue.put_nowait(packed_data)
            except asyncio.QueueFull:
                continue

    def start(self):
        asyncio.ensure_future(self.send_data())
        asyncio.ensure_future(self.push_metrics())
