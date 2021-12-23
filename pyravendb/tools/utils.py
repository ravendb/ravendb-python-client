from pyravendb.custom_exceptions import exceptions
import OpenSSL.crypto
try:
    from collections.abc import Iterable
except ImportError:
    from collections import Iterable
from pyravendb.tools.projection import create_entity_with_mapper
from datetime import datetime, timedelta
from enum import Enum
from threading import Timer
from copy import deepcopy
import urllib
import inspect
import json
import sys
import re


class _DynamicStructure(object):
    def __init__(self, **entries):
        self.__dict__.update(entries)

    def __str__(self):
        return str(self.__dict__)


class Utils(object):
    @staticmethod
    def quote_key(key, reserved_slash=False):
        reserved = '%:=&?~#+!$,;\'*[]'
        if reserved_slash:
            reserved += '/'
        if key:
            return urllib.parse.quote(key, safe=reserved)
        else:
            return ''

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
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

    @staticmethod
    def database_name_validation(name):
        if name is None:
            raise ValueError("None name is not valid")
        result = re.match(r'([A-Za-z0-9_\-\.]+)', name, re.IGNORECASE)
        if not result:
            raise exceptions.InvalidOperationException(
                "Database name can only contain only A-Z, a-z, \"_\", \".\" or \"-\" but was: " + name)

    @staticmethod
    def first_or_default(iterator, func, default):
        for item in iterator:
            if func(item):
                return item
        return default

    @staticmethod
    def get_change_vector_from_header(response):
        header = response.headers.get("ETag", None)
        if header is not None and header[0] == "\"":
            return header[1: len(header) - 2]

    @staticmethod
    def import_class(name):
        components = name.split('.')
        module_name = '.'.join(name.split('.')[:-1])
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
    def initialize_object(obj, object_type, convert_to_snake_case=None):
        initialize_dict, set_needed = Utils.make_initialize_dict(obj, object_type.__init__, convert_to_snake_case)
        o = object_type(**initialize_dict)
        if set_needed:
            for key, value in obj.items():
                setattr(o, key, value)
        return o

    @staticmethod
    def convert_to_entity(document, object_type, conventions, events, nested_object_types=None):

        metadata = document.pop("@metadata")
        original_document = deepcopy(document)
        original_metadata = deepcopy(metadata)
        type_from_metadata = conventions.try_get_type_from_metadata(metadata)
        mapper = conventions.mappers.get(object_type, None)

        events.before_conversion_to_entity(document, metadata, type_from_metadata)

        if object_type == dict:
            events.after_conversion_to_entity(document, document, metadata)
            return document, metadata, original_metadata, original_document

        if type_from_metadata is None:
            if object_type is not None:
                metadata["Raven-Python-Type"] = "{0}.{1}".format(object_type.__module__, object_type.__name__)
            else:  # no type defined on document or during load, return a dict
                dyn = _DynamicStructure(**document)
                events.after_conversion_to_entity(dyn, document, metadata)
                return dyn, metadata, original_metadata, original_document
        else:
            object_from_metadata = Utils.import_class(type_from_metadata)
            if object_from_metadata is not None:
                if object_type is None:
                    object_type = object_from_metadata

                elif Utils.is_inherit(object_type, object_from_metadata):
                    mapper = conventions.mappers.get(object_from_metadata, None) or mapper
                    object_type = object_from_metadata
                elif object_type is not object_from_metadata:
                    raise exceptions.InvalidOperationException(
                        f"Cannot covert document from type {object_from_metadata} to {object_type}")

        if nested_object_types is None and mapper:
            entity = create_entity_with_mapper(document, mapper, object_type)
        else:
            entity = _DynamicStructure(**document)
            entity.__class__ = object_type

            entity = Utils.initialize_object(document, object_type)

            if nested_object_types:
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
                                setattr(entity, key, Utils.initialize_object(attr, nested_object_types[key]))
                        except TypeError as e:
                            print(e)
                            pass

        if 'Id' in entity.__dict__:
            entity.Id = metadata.get('@id', None)
        events.after_conversion_to_entity(entity, document, metadata)
        return entity, metadata, original_metadata, original_document

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
                    converted_document[Utils.convert_to_snake_case(converted_key)] = document[key]
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
        return bytes(json_dict, encoding='utf-8')

    @staticmethod
    def dict_to_string(dictionary):
        builder = []
        for item in dictionary:
            if sys.version_info.major > 2 and isinstance(dictionary[item], bytes):
                dictionary[item] = dictionary[item].decode('utf-8')
            builder.append('{0}={1}'.format(item, dictionary[item]))
        return ','.join(item for item in builder)

    @staticmethod
    def datetime_to_string(datetime_obj):
        add_suffix = '0' if datetime_obj != datetime.max else '9'
        return datetime_obj.strftime(f"%Y-%m-%dT%H:%M:%S.%f{add_suffix}") if datetime_obj else ''

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
        try:
            if datetime_str.endswith('Z'):
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
        pattern = r'(?:(-?\d+)[.])?(\d{2}):(\d{2}):(\d{2})(?:.(\d+))?'
        timedelta_initialize = None
        m = re.match(pattern, timedelta_str, re.IGNORECASE)
        if m:
            timedelta_initialize = {"days": 0 if m.group(1) is None else int(m.group(1)),
                                    "hours": 0 if m.group(2) is None else int(m.group(2)),
                                    "minutes": 0 if m.group(3) is None else int(m.group(3)),
                                    "seconds": 0 if m.group(4) is None else int(m.group(4)),
                                    "microseconds": 0 if m.group(5) is None else int(m.group(5))
                                    }
        if timedelta_initialize:
            return timedelta(**timedelta_initialize)
        return None

    @staticmethod
    def timedelta_to_str(timedelta_obj):
        timedelta_str = ""
        if isinstance(timedelta_obj, timedelta):
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
                timedelta_str += ".{0}".format(microseconds)
        return timedelta_str

    @staticmethod
    def escape(term, allow_wild_cards, make_phrase):
        wild_cards = ['-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', ':', '\\']
        if not term:
            return "\"\""
        start = 0
        length = len(term)
        buffer = ""
        if length >= 2 and term[0] == '/' and term[1] == '/':
            buffer += "//"
            start = 2
        i = start
        while i < length:
            ch = term[i]
            if ch == '*' or ch == '?':
                if allow_wild_cards:
                    i += 1
                    continue

            if ch in wild_cards:
                if i > start:
                    buffer += term[start:i - start]

                buffer += '\\{0}'.format(ch)
                start = i + 1

            elif ch == ' ' or ch == '\t':
                if make_phrase:
                    return "\"{0}\"".format(Utils.escape(term, allow_wild_cards, False))

            i += 1
        if length > start:
            buffer += term[start: length - start]

        return buffer

    @staticmethod
    def pfx_to_pem(pem_path, pfx_path, pfx_password):
        """
        @param pem_path: Where to create the pem file
        @param pfx_path: The path to the pfx file
        @param pfx_password: The password to pfx file
        """
        with open(pem_path, 'wb') as pem_file:
            with open(pfx_path, 'rb') as pfx_file:
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
        with open(pem_path, 'rb') as pem_file:
            cert = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, pem_file.read())
            return str(cert.digest("sha1"))

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
        elif getattr(o, "__dict__", None):
            return o.__dict__
        elif isinstance(o, set):
            return list(o)
        elif isinstance(o, int) or isinstance(o, float):
            return str(o)
        else:
            raise TypeError(repr(o) + " is not JSON serializable")
        