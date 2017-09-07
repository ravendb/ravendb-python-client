from pyravendb.custom_exceptions import exceptions
import OpenSSL.crypto
from collections import Iterable
from datetime import datetime, timedelta
from threading import Timer
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
        if parent == child:
            return True
        if child is None:
            return False
        if parent != child:
            return Utils.is_inherit(parent, child.__base__)

    @staticmethod
    def convert_to_entity(document, object_type, conventions, nested_object_types=None, fetch=False):
        metadata = document.pop("@metadata")
        original_metadata = metadata.copy()
        type_from_metadata = conventions.try_get_type_from_metadata(metadata)
        if object_type == dict:
            return document, metadata, original_metadata
        entity = _DynamicStructure(**document)
        if not fetch:
            object_from_metadata = None
            if type_from_metadata is not None:
                object_from_metadata = Utils.import_class(type_from_metadata)

            if object_from_metadata is None:
                if object_type is not None:
                    entity.__class__ = object_type
                    metadata["Raven-Python-Type"] = "{0}.{1}".format(object_type.__module__, object_type.__name__)
            else:
                if object_type and not Utils.is_inherit(object_type, object_from_metadata):
                    raise exceptions.InvalidOperationException(
                        "Unable to cast object of type {0} to type {1}".format(object_from_metadata, object_type))
                entity.__class__ = object_from_metadata
            # Checking the class for initialize
            entity_initialize_dict = Utils.make_initialize_dict(document, entity.__class__.__init__)

            entity = entity.__class__(**entity_initialize_dict)

        if nested_object_types:
            for key in nested_object_types:
                attr = getattr(entity, key)
                if attr:
                    try:
                        if isinstance(attr, list):
                            nested_list = []
                            for attribute in attr:
                                nested_list.append(nested_object_types[key](
                                    **Utils.make_initialize_dict(attribute, nested_object_types[key].__init__)))
                            setattr(entity, key, nested_list)

                        elif nested_object_types[key] is datetime:
                            setattr(entity, key, Utils.string_to_datetime(attr))
                        elif nested_object_types[key] is timedelta:
                            setattr(entity, key, Utils.string_to_timedelta(attr))
                        else:
                            setattr(entity, key, nested_object_types[key](
                                **Utils.make_initialize_dict(attr, nested_object_types[key].__init__)))
                    except TypeError:
                        pass

        if 'Id' in entity.__dict__:
            entity.Id = metadata.get('@id', None)
        return entity, metadata, original_metadata

    @staticmethod
    def make_initialize_dict(document, entity_init):
        if entity_init is None:
            return document

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

        return entity_initialize_dict

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
        return datetime_obj.strftime("%Y-%m-%dT%H:%M:%S.%f0")

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
    def escape_if_needed(name):
        if name:
            escape = False
            first = True
            for c in name:
                if first:
                    if not c.isalpha() and c != '_' and c != '@':
                        escape = True
                        break
                    first = False
                else:
                    if (not c.isalpha() and not c.isdigit()) and c != '_' and c != '@' and c != '[' and c != ']':
                        escape = True
                        break

            if escape:
                return "'{0}'".format(name)
        return name

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
    def index_of_any(text, any_of):
        """
        @param text: The text we want to check
        :type str
        @param any_of: list of char
        :type list
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
