"""
Cursor utils, copied from
https://github.com/djrobstep/sqlakeyset/blob/master/sqlakeyset/results.py
"""
import base64
import csv
from typing import Any, Generic, List, Optional, Tuple, Type, TypeVar, Callable

from pydantic import BaseModel

from beaniekeyset.serializer import BadBookmarkError, Serial

T = TypeVar("T", bound=BaseModel)


SERIALIZER_SETTINGS = {
    "lineterminator": "",
    "delimiter": "~",
    "doublequote": False,
    "escapechar": "\\",
    "quoting": csv.QUOTE_NONE,
}

serializer = Serial(**SERIALIZER_SETTINGS)


def custom_bookmark_type(
    type: Type[T],  # TODO: rename this in a major release
    code: str,
    d: Optional[Callable[[str], T]] = None,
    s: Optional[Callable[[T], str]] = None,
):
    """Register (de)serializers for bookmarks to use for a custom type.

    :param type: Python type to register.
    :paramtype type: type
    :param code: A short alphabetic code to use to identify this type in serialized bookmarks.
    :paramtype code: str
    :param serializer: A function mapping `type` values to strings. Default is
        `str`.
    :param deserializer: Inverse for `serializer`. Default is the `type`
        constructor."""
    serializer.register_type(type, code, deserializer=d, serializer=s)


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


class BeaniePage(Generic[T]):
    def __init__(
        self,
        documents: List[Any],  # Fix this typing
        per_page: int,
        ordering_fields: List[Tuple[str, Any]],
        backwards: bool,
        original_model: Type[T],
        current_cursor: Optional[str] = None,
    ) -> None:
        current_marker, _ = (
            unserialize_bookmark(current_cursor) if current_cursor else None,
            None,
        )
        self.original_documents = documents
        self.ordering_fields = ordering_fields
        self.per_page = per_page
        self.backwards = backwards
        self.current_marker = current_marker
        self.markers = [
            tuple([doc.dict(by_alias=True)[field] for field, _ in self.ordering_fields])
            for doc in self.original_documents
        ]
        self.marker_0 = current_marker
        self.documents = [
            original_model(**document.dict(by_alias=True))
            for document in documents[:per_page]
        ]
        excess = documents[per_page:]
        self.marker_1: Optional[Tuple[Any]] = None
        self.marker_n: Optional[Tuple[Any]] = None
        self.marker_nplus1: Optional[Tuple[Any]] = None
        if self.documents:
            self.marker_1 = self.markers[0]
            self.marker_n = self.markers[len(self.documents) - 1]
        if excess:
            self.marker_nplus1 = self.markers[len(self.documents)]

        four = [self.marker_0, self.marker_1, self.marker_n, self.marker_nplus1]
        if backwards:
            self.markers.reverse()
            self.documents.reverse()
            four.reverse()
        self._previous, self._first, self._last, self._next = four

    @property
    def has_next(self):
        """
        Boolean flagging whether there are more rows after this page (in the
        original query order).
        """
        return bool(self._next)

    @property
    def has_previous(self):
        """
        Boolean flagging whether there are more rows before this page (in the
        original query order).
        """
        return bool(self._previous)

    @property
    def last(self):
        """Marker for the next page (in the original query order)."""
        return self._last, False

    @property
    def first(self):
        """Marker for the previous page (in the original query order)."""
        return self._first, True

    @property
    def previous(self):
        return self._previous, True

    @property
    def next(self):
        return self._next, False

    @property
    def current(self):
        """Marker for the current page in the current paging direction."""
        if self.backwards:
            return self.previous
        else:
            return self.next

    @property
    def current_opposite(self):
        """
        Marker for the current page in the opposite of the current
        paging direction.
        """
        if self.backwards:
            return self.next
        else:
            return self.previous

    @property
    def further(self):
        """Marker for the following page in the current paging direction."""
        if self.backwards:
            return self.previous
        else:
            return self.next

    @property
    def has_further(self) -> bool:
        """
        Boolean flagging whether there are more rows before this page in the
        current paging direction.
        """
        if self.backwards:
            return self.has_previous
        else:
            return self.has_next

    @property
    def is_full(self) -> bool:
        """
        Boolean flagging whether this page contains as many rows as were
        requested in ``per_page``.
        """
        return len(self.documents) == self.per_page

    @property
    def all_bookmarks(self) -> List[str]:
        return [serialize_bookmark(((marker), False)) for marker in self.markers]

    @property
    def documents_with_cursors(self) -> List[Tuple[str, T]]:
        return list(zip(self.all_bookmarks[: len(self.documents)], self.documents))

    @property
    def bookmark_first(self) -> str:
        return serialize_bookmark(self.first)

    @property
    def bookmark_last(self) -> str:
        return serialize_bookmark(self.last)

    @property
    def bookmark_previous(self) -> str:
        return serialize_bookmark(self.previous)

    @property
    def bookmark_next(self) -> str:
        return serialize_bookmark(self.next)

    def get_place(self, bookmark) -> tuple:
        marker = unserialize_bookmark(bookmark)
        place, _ = marker
        if not place:
            return tuple()
        return tuple(place)
