"""
Bookmark (de)serialization logic.
Copied from https://github.com/djrobstep/sqlakeyset/blob/master/sqlakeyset/serial/serial.py
"""

import base64
import csv
import datetime
import decimal
import uuid
from io import StringIO
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Type

from dateutil import parser


class InvalidPageError(Exception):
    """
    An invalid page marker (in either tuple or bookmark string form) was
    provided to a paging method.
    """


class BadBookmarkError(InvalidPageError):
    """A bookmark string failed to parse"""


class PageSerializationError(Exception):
    """Generic serialization error."""


class UnregisteredType(Exception):
    """An unregistered type was encountered when serializing a bookmark."""


class ConfigurationError(Exception):
    """An error to do with configuring custom bookmark types."""


NONE = "x"
TRUE = "true"
FALSE = "false"
STRING = "s"
BINARY = "b"
INTEGER = "i"
FLOAT = "f"
DECIMAL = "n"
DATE = "d"
DATETIME = "dt"
TIME = "t"
UUID = "uuid"


def parsedate(x):
    return parser.parse(x).date()


def binencode(x):
    return base64.b64encode(x).decode("utf-8")


def bindecode(x):
    return base64.b64decode(x.encode("utf-8"))


TYPES = [
    (str, "s"),
    (int, "i"),
    (float, "f"),
    (bytes, "b", bindecode, binencode),
    (decimal.Decimal, "n"),
    (uuid.UUID, "uuid"),
    (datetime.datetime, "dt", parser.parse),
    (datetime.date, "d", parsedate),
    (datetime.time, "t"),
]

BUILTINS = {
    "x": None,
    "true": True,
    "false": False,
}
BUILTINS_INV = {v: k for k, v in BUILTINS.items()}


class Serial:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        self.serializers: Dict[Type, Callable[..., Tuple[str, Any]]] = {}
        self.deserializers: Dict[str, Callable[..., Any]] = {}
        for definition in TYPES:
            self.register_type(*definition)

    def register_type(
        self,
        type: Type,
        code: str,
        deserializer: Optional[Callable] = None,
        serializer: Optional[Callable] = None,
    ):
        _deserializer = deserializer
        _serializer = serializer
        if _serializer is None:
            _serializer = str
        if _deserializer is None:
            _deserializer = type
        if type in self.serializers:
            raise ConfigurationError("Type {type} already has a serializer registered.")
        if code in self.deserializers:
            raise ConfigurationError("Type code {code} is already in use.")
        self.serializers[type] = lambda x: (code, _serializer(x))
        self.deserializers[code] = _deserializer

    def split(self, joined: str) -> List[str]:
        string_io = StringIO(joined)
        reader = csv.reader(string_io, **self.kwargs)
        row = next(reader)
        return row

    def join(self, string_list: Iterable[Any]) -> str:
        string_io = StringIO()
        writer = csv.writer(string_io, **self.kwargs)
        writer.writerow(string_list)
        return string_io.getvalue()

    def serialize_values(self, values: Iterable[Any]) -> str:
        if values is None:
            return ""
        return self.join(self.serialize_value(_) for _ in values)

    def unserialize_values(self, cursor: str) -> List[Any]:
        if cursor == "":
            return []

        return [self.unserialize_value(_) for _ in self.split(cursor)]

    def serialize_value(self, x: Any) -> str:
        try:
            serializer = self.serializers[type(x)]
        except KeyError:
            pass  # fall through to builtins
        else:
            try:
                c, x = serializer(x)
            except Exception as e:
                raise PageSerializationError(
                    "Custom bookmark serializer " "encountered error"
                ) from e
            else:
                return f"{c}:{x}"

        try:
            return BUILTINS_INV[x]
        except KeyError:
            raise UnregisteredType(
                "Don't know how to serialize type of {} ({}). "
                "Use custom_bookmark_type to register it.".format(x, type(x))
            )

    def unserialize_value(self, cursor: str) -> Any:
        try:
            code, value = cursor.split(":", 1)
        except ValueError:
            code = cursor
            value = None

        try:
            deserializer = self.deserializers[code]
        except KeyError:
            pass  # fall through to builtins
        else:
            try:
                return deserializer(value)
            except Exception as e:
                raise BadBookmarkError(
                    "Custom bookmark deserializer" "encountered error"
                ) from e

        try:
            return BUILTINS[code]
        except KeyError:
            raise BadBookmarkError(f"unrecognized value {cursor}")
