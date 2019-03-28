from typing import NamedTuple


class Message(NamedTuple):

    payload: dict
    filters: dict
