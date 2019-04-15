import logging
from asyncio import Queue
from typing import TYPE_CHECKING, Any, Dict, Set

QUEUE_LOGGER = logging.getLogger("crispy.ClientQueue")


if TYPE_CHECKING:
    # pylint: disable=unused-import,ungrouped-imports
    from crispywaffle.models import Message


class Client:

    def __init__(self, filters: Dict[str, str], storage: "ClientPool") -> None:
        self.filters = filters
        self.queue: Queue = Queue()
        self.storage: "ClientPool" = storage

    def __enter__(self) -> "Client":
        QUEUE_LOGGER.debug("Registering client %s", self.filters)
        self.storage.add_client(self)
        return self

    def __exit__(self, *args) -> None:
        QUEUE_LOGGER.debug("Removing client %s", self.filters)
        self.storage.discard_client(self)

    def match(self, properties: Dict[str, str]) -> bool:
        for key, value in properties.items():
            if key not in self.filters or self.filters[key] != value:
                return False
        return True

    def put(self, item: Any) -> None:
        self.queue.put_nowait(item)


class ClientPool:

    def __init__(self):
        self.clients: Set["Client"] = set()

    def add_client(self, client: "Client"):
        self.clients.add(client)

    def discard_client(self, client: "Client"):
        self.clients.discard(client)

    def matching_clients_iter(self, match: Dict):
        for client in self.clients:
            if client.match(match):
                yield client

    def put_message(self, message: "Message"):
        for client in self.matching_clients_iter(message.filters):
            client.put(message.payload)
