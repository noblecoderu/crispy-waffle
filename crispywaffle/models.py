from typing import NamedTuple, TYPE_CHECKING


if TYPE_CHECKING:
    # pylint: disable=unused-import,ungrouped-imports
    from uuid import UUID


class Message(NamedTuple):

    uuid: "UUID"
    payload: dict
    filters: dict
