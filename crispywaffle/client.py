import logging
from asyncio import Queue
from typing import Dict, Iterable, List

CLIENT_QUEUES: List["ClientQueue"] = []
QUEUE_LOGGER = logging.getLogger("crispy.ClientQueue")


class ClientQueue:  # pylint: disable=too-few-public-methods

    def __init__(self, filters: Dict[str, str]) -> None:
        self.filters = filters
        self.queue = Queue()

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


def match_client_queue(properties: Dict[str, str]) -> Iterable[Queue]:
    for _ in filter(lambda c: c.match(properties), CLIENT_QUEUES):
        yield _[1]
