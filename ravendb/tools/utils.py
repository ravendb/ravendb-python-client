from __future__ import annotations

import enum
import time
from typing import (
    Optional,
    Dict,
    Generic,
    Tuple,
    TypeVar,
    Collection,
    List,
    Union,
    Type,
    Any,
    Callable,
)

import requests

from ravendb.primitives import constants
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary

try:
    from collections.abc import Iterable, Sequence
except ImportError:
    from collections import Iterable, Sequence

from datetime import datetime, timedelta
from enum import Enum
from threading import Timer
import urllib
import inspect
import json
import sys
import re

_T = TypeVar("_T")
_T2 = TypeVar("_T2")
_TKey = TypeVar("_TKey")
_TVal = TypeVar("_TVal")

_default_wildcards = {
    "-",
    "&",
    "|",
    "!",
    "(",
    ")",
    "{",
    "}",
    "[",
    "]",
    "^",
    '"',
    "'",
    "~",
    ":",
    "\\",
}


class TimeUnit(Enum):
    NANOSECONDS = 1
    MICROSECONDS = 1000 * NANOSECONDS
    MILLISECONDS = 1000 * MICROSECONDS
    SECONDS = 1000 * MILLISECONDS
    MINUTES = 60 * SECONDS
    HOURS = 60 * MINUTES
    DAYS = 24 * HOURS

    def convert(self, source_duration: int, source_unit: TimeUnit):
        if self == TimeUnit.NANOSECONDS:
            return source_unit.to_nanos(source_duration)
        elif self == TimeUnit.MICROSECONDS:
            return source_unit.to_micros(source_duration)
        elif self == TimeUnit.MILLISECONDS:
            return source_unit.to_millis(source_duration)
        elif self == TimeUnit.SECONDS:
            return source_unit.to_seconds(source_duration)
        raise NotImplementedError("Unsupported TimeUnit")

    def to_nanos(self, duration: int) -> int:
        return duration * self.value

    def to_micros(self, duration: int) -> int:
        return duration * self.value // 1000

    def to_millis(self, duration: int) -> int:
        return duration * self.value // (1000 * 1000)

    def to_seconds(self, duration: int) -> int:
        return duration * self.value // (1000 * 1000 * 1000)

    def to_minutes(self, duration: int) -> int:
        return duration * self.value // (1000 * 1000 * 1000 * 60)

    def to_hours(self, duration: int) -> int:
        return duration * self.value // (1000 * 1000 * 1000 * 60 * 60)

    def to_days(self, duration: int) -> int:
        return duration * self.value // (1000 * 1000 * 1000 * 60 * 60 * 24)


class Stopwatch:
    def __init__(self):
        self.__is_running = False
        self.__elapsed_nanos = 0
        self.__start_tick = None

    @staticmethod
    def create_started() -> Stopwatch:
        return Stopwatch().start()

    @property
    def is_running(self) -> bool:
        return self.__is_running

    def start(self) -> Stopwatch:
        if self.__is_running:
            raise RuntimeError("This stopwatch is already running.")
        self.__is_running = True
        self.__start_tick = time.perf_counter_ns()
        return self

    def stop(self) -> Stopwatch:
        tick = time.perf_counter_ns()
        if not self.__is_running:
            raise RuntimeError("This stopwatch is already stopped.")
        self.__is_running = False
        self.__elapsed_nanos = (
            (tick - self.__start_tick)
            if self.__elapsed_nanos is None
            else self.__elapsed_nanos + (tick - self.__start_tick)
        )
        return self

    def reset(self) -> Stopwatch:
        self.__elapsed_nanos = 0
        self.__is_running = False
        return self

    def __take_elapsed_nanos(self) -> float:
        return (
            time.perf_counter_ns() - self.__start_tick + self.__elapsed_nanos
            if self.__is_running
            else self.__elapsed_nanos
        )

    def elapsed_micros(self) -> float:
        return self.__take_elapsed_nanos() / 1000

    def elapsed(self, desired_unit: Optional[TimeUnit] = None) -> Union[timedelta, int]:
        if desired_unit is not None:
            return desired_unit.convert(self.__elapsed_nanos, TimeUnit.NANOSECONDS)
        return timedelta(microseconds=self.elapsed_micros())


class Size:
    def __init__(self, size_in_bytes: int = None, human_size: str = None):
        self.size_in_bytes = size_in_bytes
        self.human_size = human_size

    @classmethod
    def from_json(cls, json_dict: Dict) -> Size:
        return cls(json_dict["SizeInBytes"], json_dict["HumaneSize"])


# todo: https://issues.hibernatingrhinos.com/issue/RDBC-686
class CaseInsensitiveDict(dict, Generic[_TKey, _TVal]):
    @classmethod
    def _lower_if_str(cls, key):
        return key.lower() if isinstance(key, str) else key

    def __init__(self, *args, **kwargs):
        super(CaseInsensitiveDict, self).__init__(*args, **kwargs)
        self._original_values: Dict[_TKey, _TKey] = {}
        self._convert_keys()

    def _convert_key_back(self, lowered_key: _TKey) -> _TKey:
        return self._original_values[lowered_key]

    def _convert_item_back(self, item: Tuple[_TKey, _TVal]) -> Tuple[_TKey, _TVal]:
        return self._convert_key_back(item[0]), item[1]

    def __getitem__(self, key) -> _TVal:
        return super(CaseInsensitiveDict, self).__getitem__(self.__class__._lower_if_str(key))

    def __setitem__(self, key, value):
        adjusted_key = self.__class__._lower_if_str(key)
        self._original_values[adjusted_key] = key
        super(CaseInsensitiveDict, self).__setitem__(adjusted_key, value)

    def __delitem__(self, key):
        return super(CaseInsensitiveDict, self).__delitem__(self.__class__._lower_if_str(key))

    def __contains__(self, key):
        return super(CaseInsensitiveDict, self).__contains__(self.__class__._lower_if_str(key))

    def pop(self, key, *args, **kwargs) -> _TVal:
        return super(CaseInsensitiveDict, self).pop(self.__class__._lower_if_str(key), *args, **kwargs)

    def remove(self, key, *args, **kwargs) -> Optional[_TVal]:
        return self.pop(key, *args, **kwargs) if key in self else None

    def get(self, key, *args, **kwargs) -> _TVal:
        return super(CaseInsensitiveDict, self).get(self.__class__._lower_if_str(key), *args, **kwargs)

    def setdefault(self, key, *args, **kwargs):
        return super(CaseInsensitiveDict, self).setdefault(self.__class__._lower_if_str(key), *args, **kwargs)

    def update(self, e=None, **f):
        super(CaseInsensitiveDict, self).update(self.__class__(e))
        super(CaseInsensitiveDict, self).update(self.__class__(**f))

    def _convert_keys(self):
        for k in list(self):
            v = super(CaseInsensitiveDict, self).pop(k)
            self.__setitem__(k, v)

    def keys(self) -> List[_TKey]:
        return list(self._original_values.values())


class CaseInsensitiveSet(set):
    @classmethod
    def _v(cls, value):
        return value.lower() if isinstance(value, str) else value

    def __init__(self, *args, **kwargs):
        super(CaseInsensitiveSet, self).__init__(*args, **kwargs)
        self._convert_values()

    def __contains__(self, value):
        return super(CaseInsensitiveSet, self).__contains__(self.__class__._v(value))

    def add(self, element) -> None:
        super().add(self._v(element))

    def discard(self, element) -> None:
        super().discard(self._v(element))

    def remove(self, element) -> None:
        super().remove(self._v(element))

    def update(self, e=None, **f):
        super(CaseInsensitiveSet, self).update(self.__class__(e))
        super(CaseInsensitiveSet, self).update(self.__class__(**f))

    def _convert_values(self):
        for v in self:
            super().discard(v)
            self.add(v)


class DynamicStructure(object):
    def __init__(self, **entries):
        self.__dict__.update(entries)

    def __str__(self):
        return str(self.__dict__)


class QueryFieldUtil:
    @staticmethod
    def _should_escape(s: str, is_path: bool) -> bool:
        escape = False
        inside_escaped = False

        first = True
        for c in s:
            if c in ["'", '"']:
                inside_escaped = not inside_escaped
                continue
            if first:
                first = False
                if not c.isalpha() and c not in ["_", "@"] and not inside_escaped:
                    escape = True
                    break
            else:
                if not c.isalnum() and c not in ["_", "-", "@", ".", "[", "]"] and not inside_escaped:
                    escape = True
                    break

                if is_path and c == "." and not inside_escaped:
                    escape = True
                    break

        escape |= inside_escaped
        return escape

    @staticmethod
    def escape_if_necessary(name: str, is_path: bool = False) -> str:
        if (
            not name
            or name.isspace()
            or name
            in [
                constants.Documents.Indexing.Fields.DOCUMENT_ID_FIELD_NAME,
                constants.Documents.Indexing.Fields.REDUCE_KEY_HASH_FIELD_NAME,
                constants.Documents.Indexing.Fields.REDUCE_KEY_KEY_VALUE_FIELD_NAME,
                constants.Documents.Indexing.Fields.VALUE_FIELD_NAME,
                constants.Documents.Indexing.Fields.SPATIAL_SHAPE_FIELD_NAME,
            ]
        ):
            return name

        if not QueryFieldUtil._should_escape(name, is_path):
            return name

        sb = [c for c in name]
        need_end_quote = False
        last_term_start = 0

        for i in range(len(sb)):
            c = sb[i]
            if i == 0 and not c.isalpha() and c not in ["_", "@"]:
                sb.insert(last_term_start, "'")
                need_end_quote = True
                continue

            if is_path and c == ".":
                if need_end_quote:
                    need_end_quote = False
                    sb.insert(i, "'")
                    i += 1

                last_term_start = i + 1
                continue

            if c.isalnum() and c not in ["_", "-", "@", ".", "[", "]"] and not need_end_quote:
                sb.insert(last_term_start, "'")
                need_end_quote = True
                continue

        if need_end_quote:
            sb.append("'")

        return "".join(sb)


class Utils(object):
    primitives = (int, float, bool, str, bytes, bytearray)
    mutable_collections = (list, set)
    collections_no_str = (list, set, tuple)
    primitives_and_collections = (int, float, bool, str, bytes, bytearray, list, set, tuple)

    @staticmethod
    def check_if_collection_but_not_str(instance) -> bool:
        return isinstance(instance, (list, set, tuple))

    @staticmethod
    def unpack_collection(items: Collection) -> List:
        results = []

        for item in items:
            if Utils.check_if_collection_but_not_str(item):
                results.extend(Utils.unpack_collection(item))
                continue
            results.append(item)

        return results

    @staticmethod
    def quote_key(key: str, reserved_slash: bool = False, reserved_at: bool = False) -> str:
        reserved = ""
        if reserved_slash:
            reserved += "/"
        if reserved_at:
            reserved += "@"
        if key:
            return urllib.parse.quote(key, safe=reserved)
        else:
            return ""

    @staticmethod
    def get_change_vector_from_header(response: requests.Response):
        header = response.headers.get("ETag", None)
        if header is not None and header[0] == '"':
            return header[1 : len(header) - 2]

    @staticmethod
    def import_class(name: str) -> Optional[Type]:
        components = name.split(".")
        module_name = ".".join(name.split(".")[:-1])
        mod = None
        try:
            mod = getattr(__import__(module_name, fromlist=[components[-1]]), components[-1])
        except (ImportError, ValueError, AttributeError):
            pass
        return mod

    @staticmethod
    def is_inherit(parent: Type[Any], child: Type[Any]) -> bool:
        if child is None or parent is None:
            return False
        if parent == child:
            return True
        if parent != child:
            return Utils.is_inherit(parent, child.__base__)

    @staticmethod
    def initialize_object(json_dict: Dict[str, Any], object_type: Type[_T]) -> _T:
        initialize_dict, should_set_object_fields = Utils.create_initialize_kwargs(json_dict, object_type.__init__)
        try:
            entity = object_type(**initialize_dict)
        except Exception as e:
            if "Id" not in initialize_dict:
                initialize_dict["Id"] = None
                entity = object_type(**initialize_dict)
            else:
                raise TypeError(
                    f"Couldn't initialize object of type '{object_type.__name__}' using dict '{initialize_dict}'"
                ) from e
        if should_set_object_fields:
            for key, value in json_dict.items():
                setattr(entity, key, value)
        return entity

    @staticmethod
    def try_get_new_instance(object_type: Type[_T]) -> _T:
        try:
            return Utils.initialize_object({}, object_type)
        except Exception as e:
            raise RuntimeError(
                f"Couldn't initialize an object of the '{object_type.__name__}' class. "
                f"Using 'None' values as arguments in the __init__ method. {e.args[0]}",
                e,
            )

    @staticmethod
    def convert_json_dict_to_object(
        json_dict: Dict[str, Any], object_type: Optional[Type[_T]] = None, projection: bool = False
    ) -> Union[DynamicStructure, _T]:
        if object_type == dict:
            return json_dict
        if object_type is None:
            return DynamicStructure(**json_dict)

        try:
            return Utils.initialize_object(json_dict, object_type)
        except TypeError as e:
            raise TypeError(f"Couldn't project results into object type '{object_type.__name}'") if projection else e

    @staticmethod
    def get_object_fields(instance: object) -> Dict[str, object]:
        return {
            name: value
            for name, value in inspect.getmembers(instance, lambda attr: not callable(attr))
            if not name.startswith("__")
        }

    @staticmethod
    def get_class_fields(object_type: Type[_T]) -> Dict[str, Any]:
        try:
            instance = Utils.try_get_new_instance(object_type)
        except Exception as e:
            raise RuntimeError(
                f"Unable to retrieve information about class fields. "
                f"Couldn't get instance of '{object_type.__qualname__}'.",
                e,
            )
        return Utils.get_object_fields(instance)

    @staticmethod
    def create_initialize_kwargs(
        document: Dict[str, Any],
        object_init_method: Callable[[Dict[str, Any]], None],
    ) -> Tuple[Dict[str, Any], bool]:
        set_needed = False
        entity_initialize_dict = {}
        args, __, keywords, defaults, _, _, _ = inspect.getfullargspec(object_init_method)
        if (len(args) - 1) > len(document):
            remainder = len(args)
            if defaults:
                remainder -= len(defaults)
            for i in range(1, remainder):
                entity_initialize_dict[args[i]] = document.get(args[i], None)
            for i in range(remainder, len(args)):
                entity_initialize_dict[args[i]] = document.get(args[i], defaults[i - remainder])
        else:
            if keywords:
                entity_initialize_dict = document
            else:
                for key in document:
                    if key in args:
                        entity_initialize_dict[key] = document[key]
            if not entity_initialize_dict and len(args) - 1 > 0:
                set_needed = True
                for key in args[1:]:
                    entity_initialize_dict[key] = None
        return entity_initialize_dict, set_needed

    @staticmethod
    def dict_to_bytes(the_dict: Dict[str, Any]):
        json_dict = json.dumps(the_dict)
        return bytes(json_dict, encoding="utf-8")

    @staticmethod
    def dict_to_string(dictionary):
        builder = []
        for item in dictionary:
            if sys.version_info.major > 2 and isinstance(dictionary[item], bytes):
                dictionary[item] = dictionary[item].decode("utf-8")
            builder.append("{0}={1}".format(item, dictionary[item]))
        return ",".join(item for item in builder)

    @staticmethod
    def datetime_to_string(datetime_obj: datetime, return_none_if_none: bool = True):
        add_suffix = "0" if datetime_obj != datetime.max else "9"
        return (
            datetime_obj.strftime(f"%Y-%m-%dT%H:%M:%S.%f{add_suffix}")
            if datetime_obj
            else None if return_none_if_none else ""
        )

    @staticmethod
    def start_a_timer(interval, function, args=None, name=None, daemon=False):
        timer = Timer(interval, function, args)
        timer.daemon = daemon
        if name is not None:
            timer.name = name
        timer.start()

        return timer

    @staticmethod
    def string_to_datetime(datetime_str):
        if datetime_str is None:
            return None
        try:
            if datetime_str.endswith("Z"):
                datetime_str = datetime_str[:-1]
            datetime_s = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S.%f")
        except ValueError:
            datetime_s = datetime.strptime(datetime_str[:-1], "%Y-%m-%dT%H:%M:%S.%f")

        return datetime_s

    @staticmethod
    def timedelta_tick(td):
        return int(td.total_seconds() * 10000000)

    @staticmethod
    def string_to_timedelta(timedelta_str):
        pattern = r"(?:(-?\d+)[.])?(\d{2}):(\d{2}):(\d{2})(?:.(\d+))?"
        timedelta_initialize = None
        m = re.match(pattern, timedelta_str, re.IGNORECASE)
        if m:
            timedelta_initialize = {
                "days": 0 if m.group(1) is None else int(m.group(1)),
                "hours": 0 if m.group(2) is None else int(m.group(2)),
                "minutes": 0 if m.group(3) is None else int(m.group(3)),
                "seconds": 0 if m.group(4) is None else int(m.group(4)),
                "microseconds": 0 if m.group(5) is None else int(m.group(5)),
            }
        if timedelta_initialize:
            return timedelta(**timedelta_initialize)
        return None

    @staticmethod
    def timedelta_to_str(timedelta_obj: timedelta):
        timedelta_str = None
        if isinstance(timedelta_obj, timedelta):
            timedelta_str = ""
            total_seconds = timedelta_obj.seconds
            days = timedelta_obj.days
            hours = total_seconds // 3600
            minutes = (total_seconds // 60) % 60
            seconds = (total_seconds % 3600) % 60
            microseconds = timedelta_obj.microseconds
            if days > 0:
                timedelta_str += "{0}.".format(days)
            timedelta_str += "{:02}:{:02}:{:02}".format(hours, minutes, seconds)
            if microseconds > 0:
                timedelta_str += f".{str(microseconds).rjust(6, '0')}"
        return timedelta_str

    @staticmethod
    def escape(term: str, allow_wild_cards: bool = False, make_phrase: bool = False) -> str:
        return Utils.__escape_internal(term, _default_wildcards if allow_wild_cards else None, make_phrase)

    @staticmethod
    def escape_skip(term: str, skipped_wild_cards: List[str], make_phrase: bool = False) -> str:
        return Utils.__escape_internal(term, _default_wildcards.difference(skipped_wild_cards), make_phrase)

    @staticmethod
    def __escape_internal(term: str, wild_cards: Collection[str] = None, make_phrase: bool = False) -> str:
        allow_wild_cards = wild_cards is None
        if wild_cards is None:
            wild_cards = []

        if not term:
            return '""'
        start = 0
        length = len(term)
        buffer = ""
        if length >= 2 and term[0] == "/" and term[1] == "/":
            buffer += "//"
            start = 2
        i = start
        while i < length:
            ch = term[i]
            if ch == "*" or ch == "?":
                if allow_wild_cards:
                    i += 1
                    continue

            if ch in wild_cards:
                if i > start:
                    buffer += term[start : i - start]

                buffer += "\\{0}".format(ch)
                start = i + 1

            elif ch == " " or ch == "\t":
                if make_phrase:
                    return f'"{Utils.escape(term, allow_wild_cards, False)}"'

            i += 1
        if length > start:
            buffer += term[start : length - start]

        return buffer

    @staticmethod
    def escape_collection_name(collection_name: str):
        special = ["'", '"', "\\"]
        position = 0
        buffer = []
        for char in collection_name:
            if char in special:
                buffer.append("\\")
            buffer.append(char)
            position += 1

        return "".join(buffer)

    @staticmethod
    def json_default(o):
        if o is None:
            return None

        if isinstance(o, datetime):
            return Utils.datetime_to_string(o)
        elif isinstance(o, timedelta):
            return Utils.timedelta_to_str(o)
        elif isinstance(o, Enum):
            return o.value
        elif isinstance(o, MetadataAsDictionary):
            return o.metadata
        elif getattr(o, "__dict__", None):
            return o.__dict__
        elif isinstance(o, set):
            return list(o)
        elif isinstance(o, int) or isinstance(o, float):
            return str(o)

        else:
            raise TypeError(repr(o) + " is not JSON serializable")

    @staticmethod
    def get_default_value(object_type: Type[_T]) -> _T:
        if object_type == bool:
            return False
        elif object_type == str:
            return ""
        elif object_type == bytes:
            return bytes(0)
        elif object_type == int:
            return int(0)
        elif object_type == float:
            return float(0)
        return None

    @staticmethod
    def object_to_dict_for_hash_calculator(obj: object) -> dict:
        object_dict = {"__name__": obj.__class__.__name__}
        object_dict.update(obj.__dict__)
        to_update = {}
        for k, v in object_dict.items():
            if v is not None and not isinstance(
                v, (bool, float, str, int, bytes, bytearray, list, set, dict, enum.Enum)
            ):
                if "__str__" in v.__dict__:
                    to_update.update({k: str(v)})
                else:
                    to_update.update({k: Utils.object_to_dict_for_hash_calculator(v)})
        object_dict.update(to_update)
        return object_dict

    @staticmethod
    def check_valid_projection(object_type_from_user: Type[Any], object_type_from_metadata: Type[Any]) -> [List[str]]:
        object_type_from_metadata_fields = Utils.get_class_fields(object_type_from_metadata)
        object_type_from_user_fields = Utils.get_class_fields(object_type_from_user)
        incompatible_fields = []
        for field_name in object_type_from_user_fields:
            if field_name not in object_type_from_metadata_fields:
                incompatible_fields.append(field_name)
        return incompatible_fields

    @staticmethod
    def entity_to_dict(entity, default_method) -> dict:
        return json.loads(json.dumps(entity, default=default_method))

    @staticmethod
    def add_hours(date: datetime, hours: int):
        return date + timedelta(hours=hours)

    @staticmethod
    def add_days(date: datetime, days: int):
        return date + timedelta(days=days)

    @staticmethod
    def add_minutes(date: datetime, minutes: int):
        return date + timedelta(minutes=minutes)

    @staticmethod
    def get_unix_time_in_ms(date: datetime) -> int:
        return int(date.timestamp() * 1000)
