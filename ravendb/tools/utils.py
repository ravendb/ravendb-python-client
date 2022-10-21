from __future__ import annotations

import enum
import time
from typing import Optional, Dict, Generic, Tuple, TypeVar, Collection, List, Union, Type, TYPE_CHECKING

from ravendb import constants
from ravendb.exceptions import exceptions
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
import OpenSSL.crypto

try:
    from collections.abc import Iterable, Sequence
except ImportError:
    from collections import Iterable, Sequence

from ravendb.tools.projection import create_entity_with_mapper
from datetime import datetime, timedelta
from enum import Enum
from threading import Timer
from copy import deepcopy
import urllib
import inspect
import json
import sys
import re

_T = TypeVar("_T")
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
if TYPE_CHECKING:
    from ravendb.documents.conventions import DocumentConventions


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
        return duration * self.value / 1000

    def to_millis(self, duration: int) -> int:
        return duration * self.value / (1000 * 1000)

    def to_seconds(self, duration: int) -> int:
        return duration * self.value / (1000 * 1000 * 1000)

    def to_minutes(self, duration: int) -> int:
        return duration * self.value / (1000 * 1000 * 1000 * 60)

    def to_hours(self, duration: int) -> int:
        return duration * self.value / (1000 * 1000 * 1000 * 60 * 60)

    def to_days(self, duration: int) -> int:
        return duration * self.value / (1000 * 1000 * 1000 * 60 * 60 * 24)


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


class CaseInsensitiveDict(dict, Generic[_TKey, _TVal]):
    @classmethod
    def _k(cls, key):
        return key.lower() if isinstance(key, str) else key

    def __init__(self, *args, **kwargs):
        super(CaseInsensitiveDict, self).__init__(*args, **kwargs)
        self._convert_keys()

    def __getitem__(self, key) -> Tuple[_TKey, _TVal]:
        return super(CaseInsensitiveDict, self).__getitem__(self.__class__._k(key))

    def __setitem__(self, key, value):
        super(CaseInsensitiveDict, self).__setitem__(self.__class__._k(key), value)

    def __delitem__(self, key):
        return super(CaseInsensitiveDict, self).__delitem__(self.__class__._k(key))

    def __contains__(self, key):
        return super(CaseInsensitiveDict, self).__contains__(self.__class__._k(key))

    def pop(self, key, *args, **kwargs) -> Tuple[_TKey, _TVal]:
        return super(CaseInsensitiveDict, self).pop(self.__class__._k(key), *args, **kwargs)

    def remove(self, key, *args, **kwargs) -> Optional[_TVal]:
        return self.pop(key, *args, **kwargs) if key in self else None

    def get(self, key, *args, **kwargs) -> _TVal:
        return super(CaseInsensitiveDict, self).get(self.__class__._k(key), *args, **kwargs)

    def setdefault(self, key, *args, **kwargs):
        return super(CaseInsensitiveDict, self).setdefault(self.__class__._k(key), *args, **kwargs)

    def update(self, e=None, **f):
        super(CaseInsensitiveDict, self).update(self.__class__(e))
        super(CaseInsensitiveDict, self).update(self.__class__(**f))

    def _convert_keys(self):
        for k in list(self):
            v = super(CaseInsensitiveDict, self).pop(k)
            self.__setitem__(k, v)


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


class _DynamicStructure(object):
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
        return isinstance(instance, (Sequence, set)) and not isinstance(instance, (str, bytes, bytearray))

    @staticmethod
    def unpack_collection(items: Collection) -> List:
        results = []

        for item in items:
            if Utils.check_if_collection_but_not_str(item):
                results.extend(Utils.__unpack_collection(item))
                continue
            results.append(item)

        return results

    @staticmethod
    def quote_key(key, reserved_slash=False, reserved_at=False) -> str:
        reserved = "%:=&?~#+!$,;'*[]"
        if reserved_slash:
            reserved += "/"
        if reserved_at:
            reserved += "@"
        if key:
            return urllib.parse.quote(key, safe=reserved)
        else:
            return ""

    @staticmethod
    def unpack_iterable(iterable):
        for item in iterable:
            if isinstance(item, Iterable) and not isinstance(item, str):
                for nested in Utils.unpack_iterable(item):
                    yield nested
            else:
                yield item

    @staticmethod
    def convert_to_snake_case(name):
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    @staticmethod
    def database_name_validation(name):
        if name is None:
            raise ValueError("None name is not valid")
        result = re.match(r"([A-Za-z0-9_\-\.]+)", name, re.IGNORECASE)
        if not result:
            raise exceptions.InvalidOperationException(
                'Database name can only contain only A-Z, a-z, "_", "." or "-" but was: ' + name
            )

    @staticmethod
    def first_or_default(iterator, func, default):
        for item in iterator:
            if func(item):
                return item
        return default

    @staticmethod
    def get_change_vector_from_header(response):
        header = response.headers.get("ETag", None)
        if header is not None and header[0] == '"':
            return header[1 : len(header) - 2]

    @staticmethod
    def import_class(name) -> Type:
        components = name.split(".")
        module_name = ".".join(name.split(".")[:-1])
        mod = None
        try:
            mod = getattr(__import__(module_name, fromlist=[components[-1]]), components[-1])
        except (ImportError, ValueError, AttributeError):
            pass
        return mod

    @staticmethod
    def is_inherit(parent, child):
        if child is None or parent is None:
            return False
        if parent == child:
            return True
        if parent != child:
            return Utils.is_inherit(parent, child.__base__)

    @staticmethod
    def fill_with_nested_object_types(entity: object, nested_object_types: Dict[str, type]) -> object:
        for key in nested_object_types:
            attr = getattr(entity, key)
            if attr:
                try:
                    if isinstance(attr, list):
                        nested_list = []
                        for attribute in attr:
                            nested_list.append(Utils.initialize_object(attribute, nested_object_types[key]))
                        setattr(entity, key, nested_list)
                    elif nested_object_types[key] is datetime:
                        setattr(entity, key, Utils.string_to_datetime(attr))
                    elif nested_object_types[key] is timedelta:
                        setattr(entity, key, Utils.string_to_timedelta(attr))
                    else:
                        setattr(
                            entity,
                            key,
                            Utils.initialize_object(attr, nested_object_types[key]),
                        )
                except TypeError as e:
                    print(e)
                    pass
        return entity

    @staticmethod
    def initialize_object(obj: dict, object_type: Type[_T], convert_to_snake_case: bool = None) -> _T:
        initialize_dict, set_needed = Utils.make_initialize_dict(obj, object_type.__init__, convert_to_snake_case)
        o = object_type(**initialize_dict)
        if set_needed:
            for key, value in obj.items():
                setattr(o, key, value)
        return o

    @staticmethod
    def get_field_names(object_type: Type[_T]) -> List[str]:
        obj = Utils.initialize_object({}, object_type)
        return list(obj.__dict__.keys())

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
        json_dict: dict, object_type: Optional[Type[_T]] = None, nested_object_types: Optional[Dict[str, type]] = None
    ) -> Union[_DynamicStructure, _T]:
        if object_type == dict:
            return json_dict

        if object_type is None:
            return _DynamicStructure(**json_dict)

        if nested_object_types is None:
            return Utils.initialize_object(json_dict, object_type, True)

        entity = _DynamicStructure(**json_dict)
        entity.__class__ = object_type
        entity = Utils.initialize_object(json_dict, object_type, True)
        if nested_object_types:
            Utils.fill_with_nested_object_types(entity, nested_object_types)
        Utils.deep_convert_to_snake_case(entity)
        return entity

    @staticmethod
    def deep_convert_to_snake_case(entity):
        if entity is None:
            return
        if type(entity) in [int, float, bool, str, set, list, tuple, dict]:
            return
        changes = {}
        for key, value in entity.__dict__.items():
            # todo: i dont' like the way we try to convert to snake case content of the object dict
            new_key = Utils.convert_to_snake_case(key)
            if key != new_key:
                changes.update({(key, new_key): value})  # collect the keys that changed (snake case conversion)
            Utils.deep_convert_to_snake_case(value)
        for keys, value in changes.items():  # erase
            entity.__dict__[keys[1]] = value
            del entity.__dict__[keys[0]]

    @staticmethod
    def get_object_fields(instance: object) -> Dict[str, object]:
        return {
            name: value
            for name, value in inspect.getmembers(instance, lambda attr: not callable(attr))
            if not name.startswith("__")
        }

    @staticmethod
    def get_class_fields(object_type: type) -> Dict[str, object]:
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
    def convert_to_entity(
        document: dict,
        object_type: Optional[Type[_T]],
        conventions: "DocumentConventions",
        events,
        nested_object_types=None,
    ) -> Union[_T, _DynamicStructure]:
        if document is None:
            return None
        metadata = document.pop("@metadata")
        original_document = deepcopy(document)
        type_from_metadata = conventions.try_get_type_from_metadata(metadata)
        mapper = conventions.mappers.get(object_type, None)

        events.before_conversion_to_entity(document, metadata, type_from_metadata)

        if object_type == dict:
            events.after_conversion_to_entity(document, document, metadata)
            return document, metadata, original_document

        if type_from_metadata is None:
            if object_type is not None:
                metadata["Raven-Python-Type"] = "{0}.{1}".format(object_type.__module__, object_type.__name__)
            else:  # no type defined on document or during load, return a dict
                dyn = _DynamicStructure(**document)
                events.after_conversion_to_entity(dyn, document, metadata)
                return dyn, metadata, original_document
        else:
            object_from_metadata = Utils.import_class(type_from_metadata)
            if object_from_metadata is not None:
                if object_type is None:
                    object_type = object_from_metadata

                elif Utils.is_inherit(object_type, object_from_metadata):
                    mapper = conventions.mappers.get(object_from_metadata, None) or mapper
                    object_type = object_from_metadata
                elif object_type is not object_from_metadata:
                    # todo: Try to parse if we use projection
                    raise exceptions.InvalidOperationException(
                        f"Cannot covert document from type {object_from_metadata} to {object_type}"
                    )

        if nested_object_types is None and mapper:
            entity = create_entity_with_mapper(document, mapper, object_type)
        else:
            entity = _DynamicStructure(**document)
            entity.__class__ = object_type

            entity = Utils.initialize_object(document, object_type)

            if nested_object_types:
                Utils.fill_with_nested_object_types(entity, nested_object_types)

        if "Id" in entity.__dict__:
            entity.Id = metadata.get("@id", None)
        events.after_conversion_to_entity(entity, document, metadata)
        return entity, metadata, original_document

    @staticmethod
    def make_initialize_dict(document, entity_init, convert_to_snake_case=None):
        """
        This method will create an entity from document
        In case convert_to_snake_case will convert document keys to snake_case
        convert_to_snake_case can be dictionary with special words you can change ex. From -> from_date
        """
        if convert_to_snake_case:
            convert_to_snake_case = {} if convert_to_snake_case is True else convert_to_snake_case
            try:
                converted_document = {}
                for key in document:
                    converted_key = convert_to_snake_case.get(key, key)
                    converted_document[
                        converted_key if key == "Id" else Utils.convert_to_snake_case(converted_key)
                    ] = document[key]
                document = converted_document
            except:
                pass

        if entity_init is None:
            return document

        set_needed = False
        entity_initialize_dict = {}
        args, __, keywords, defaults, _, _, _ = inspect.getfullargspec(entity_init)
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
    def dict_to_bytes(the_dict):
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
    def datetime_to_string(datetime_obj: datetime):
        add_suffix = "0" if datetime_obj != datetime.max else "9"
        return datetime_obj.strftime(f"%Y-%m-%dT%H:%M:%S.%f{add_suffix}") if datetime_obj else ""

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
                    return '"{0}"'.format(Utils.escape(term, allow_wild_cards, False))

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
    def pfx_to_pem(pem_path, pfx_path, pfx_password):
        """
        @param pem_path: Where to create the pem file
        @param pfx_path: The path to the pfx file
        @param pfx_password: The password to pfx file
        """
        with open(pem_path, "wb") as pem_file:
            with open(pfx_path, "rb") as pfx_file:
                pfx = pfx_file.read()
            p12 = OpenSSL.crypto.load_pkcs12(pfx, pfx_password)
            pem_file.write(OpenSSL.crypto.dump_privatekey(OpenSSL.crypto.FILETYPE_PEM, p12.get_privatekey()))
            pem_file.write(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, p12.get_certificate()))
            ca = p12.get_ca_certificates()
            if ca is not None:
                for cert in ca:
                    # In python 3.6* we need to save the ca to ?\lib\site-packages\certifi\cacert.pem.
                    pem_file.write(OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_PEM, cert))
        return pem_path

    @staticmethod
    def get_cert_file_fingerprint(pem_path):
        with open(pem_path, "rb") as pem_file:
            cert = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, pem_file.read())
            return str(cert.digest("sha1"))

    @staticmethod
    def pem_to_crt_and_key(pem_path: str) -> Tuple[str, str]:
        with open(pem_path, "rb") as file:
            content = file.read()
            if "BEGIN CERTIFICATE" not in content:
                raise ValueError(
                    f"Invalid file. File stored under the path '{pem_path}' isn't valid .pem certificate. "
                    f"BEGIN CERTIFICATE header wasn't found."
                )
            if "BEGIN RSA PRIVATE KEY" not in content:
                raise ValueError(
                    f"Invalid file. File stored under the path '{pem_path}' isn't valid .pem certificate. "
                    f"BEGIN RSA PRIVATE KEY header wasn't found."
                )

            content = content.decode("utf-8")
            crt = content.split("-----BEGIN RSA PRIVATE KEY-----")[0]
            key = content.split("-----END CERTIFICATE-----")[1]

        return crt, key

    @staticmethod
    def index_of_any(text, any_of):
        """
        @param str text: The text we want to check
        @param list any_of: list of char
        :returns False if text nit
        """
        result = -1
        if not text or not any_of:
            return result

        any_of = list(set(any_of))
        i = 0
        while i < len(text) and result == -1:
            for c in any_of:
                if c == text[i]:
                    result = i
                    break
            i += 1
        return result

    @staticmethod
    def contains_any(text, any_of):
        """
        @param text: The text we want to check
        :type str
        @param any_of: list of char
        :type list
        :returns False if text nit
        """
        if not text or not any_of:
            return False

        any_of = list(set(any_of))
        for c in any_of:
            if c in text:
                return True
        return False

    @staticmethod
    def sort_iterable(iterable, key=None):
        """
        This function will take an iterable and try to sort it by the key
        if the key is not given will use the sorted default one.
        return a generator
        """
        if not iterable:
            return iterable
        yield sorted(iterable, key=key)

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
    def dictionarize(obj: object) -> dict:
        dictionarized = {"__name__": obj.__class__.__name__}
        dictionarized.update(obj.__dict__)
        to_update = {}
        for k, v in dictionarized.items():
            if v is not None and not isinstance(
                v, (bool, float, str, int, bytes, bytearray, list, set, dict, enum.Enum)
            ):
                if "__str__" in v.__dict__:
                    to_update.update({k: str(v)})
                else:
                    to_update.update({k: Utils.dictionarize(v)})
        dictionarized.update(to_update)
        return dictionarized

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
