"""
Cursor utils, copied from
https://github.com/djrobstep/sqlakeyset/blob/master/sqlakeyset/results.py
"""
import base64
import csv
from typing import Any, List, Tuple

from beaniekeyset.serializer import BadBookmarkError, Serial

SERIALIZER_SETTINGS = {
    "lineterminator": "",
    "delimiter": "~",
    "doublequote": False,
    "escapechar": "\\",
    "quoting": csv.QUOTE_NONE,
}

serializer = Serial(**SERIALIZER_SETTINGS)


def serialize_bookmark(marker: Tuple[Tuple[Any], bool]) -> str:
    """
    Serialize the given bookmark.
    Args:
        marker: A pair `(keyset, backwards)`, where ``keyset`` is a tuple containing values of the ordering columns,
                and `backwards` denotes the paging direction.
    Returns:
        A serialized string.
    """
    keyset, backwards = marker
    ss = serializer.serialize_values(keyset)
    direction = "<" if backwards else ">"
    full_string = direction + ss
    return base64.b64encode(full_string.encode()).decode()


def unserialize_bookmark(bookmark: str) -> Tuple[List[Any], bool]:
    decoded = base64.b64decode(bookmark.encode()).decode()
    direction = decoded[0]
    if direction not in (">", "<"):
        raise BadBookmarkError(
            "Malformed bookmark string: doesn't start with a direction marker"
        )
    backwards = direction == "<"
    cells = serializer.unserialize_values(decoded[1:])
    return cells, backwards
