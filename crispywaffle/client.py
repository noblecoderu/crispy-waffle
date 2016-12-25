import logging
from asyncio import Queue
from typing import Dict, List, Tuple

CLIENT_QUEUES = []  # type: List[Tuple[Dict[str, str], Queue]]
QUEUE_LOGGER = logging.getLogger("crispy.client")


class ClientQueue:  # pylint: disable=too-few-public-methods

    def __init__(self, data: Dict[str, str]) -> None:
        self.data = data
        self._info_q_pair = None  # type: Tuple[Dict[str, str], Queue]

    def __enter__(self) -> Queue:
        _client_q = Queue()  # type: Queue
        self._info_q_pair = (self.data, _client_q)
        QUEUE_LOGGER.debug("Registering client %s", self.data)
        CLIENT_QUEUES.append(self._info_q_pair)
        return _client_q

    def __exit__(self, *args) -> None:
        QUEUE_LOGGER.debug("Removing client %s", self.data)
        CLIENT_QUEUES.remove(self._info_q_pair)


def match_client_queue(properties: Dict[str, str]) -> List[Queue]:
    def predicate(client: Tuple[Dict[str, str], Queue]):
        for key, value in properties.items():
            if key not in client[0]:
                return False
            if client[0][key] != value:
                return False
        return True

    return [
        _[1]
        for _ in filter(predicate, CLIENT_QUEUES)
    ]
