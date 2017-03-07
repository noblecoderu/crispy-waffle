import logging
from asyncio import Queue
from typing import Dict, Iterable, List, Any, TypeVar

CLIENT_QUEUES: List[Any] = []
QUEUE_LOGGER = logging.getLogger("crispy.ClientQueue")

JSONSerializable = TypeVar("JSONSerializable", dict, list, int, float, None)


class ClientQueue:

    def __init__(self, filters: Dict[str, str]) -> None:
        self.filters = filters
        self.queue: Queue = Queue()

    def __enter__(self) -> Queue:
        QUEUE_LOGGER.debug("Registering client %s", self.filters)
        CLIENT_QUEUES.append(self)
        return self.queue

    def __exit__(self, *args) -> None:
        QUEUE_LOGGER.debug("Removing client %s", self.filters)
        CLIENT_QUEUES.remove(self)

    def match(self, properties: Dict[str, str]) -> bool:
        for key, value in properties.items():
            if key not in self.filters or self.filters[key] != value:
                return False
        return True

    def put(self, item: JSONSerializable) -> None:
        self.queue.put_nowait(item)


def match_client(properties: Dict[str, str]) -> Iterable[ClientQueue]:
    for client in CLIENT_QUEUES:
        if client.match(properties):
            QUEUE_LOGGER.debug(
                "Matched client: %s against %s",
                properties, client.filters
            )
            yield client
        else:
            QUEUE_LOGGER.debug(
                "Ignored client: %s against %s",
                properties, client.filters
            )
            yield client
