from pyravendb.data.operations import BulkOperationOption
from pyravendb.data.indexes import IndexQuery
from pyravendb.custom_exceptions import exceptions
from datetime import datetime, timedelta
import urllib
import inspect
import sys
import re


class _DynamicStructure(object):
    def __init__(self, **entries):
        self.__dict__.update(entries)

    def __str__(self):
        return str(self.__dict__)


class Utils(object):
    @staticmethod
    def empty_etag():
        return '00000000-0000-0000-0000-000000000000'

    @staticmethod
    def quote_key(key):
        reserved = '%:=&?~#+!$,;\'*[]'
        if key:
            # To be able to work on python 2.x and 3.x
            if sys.version_info.major > 2:
                return urllib.parse.quote(key, safe=reserved)
            else:
                return urllib.quote(key, safe=reserved)
        else:
            return ''

    @staticmethod
    def name_validation(name):
        if name is None:
            raise ValueError("None name is not valid")
        result = re.match(r'([A-Za-z0-9_\-\.]+)', name, re.IGNORECASE)
        if not result:
            raise exceptions.InvalidOperationException(
                "Database name can only contain only A-Z, a-z, \"_\", \".\" or \"-\" but was: " + name)

    @staticmethod
    def build_path(index_name, query, options):
        if index_name is None:
            raise ValueError("None index_name is not valid")
        path = "bulk_docs/{0}?".format(Utils.quote_key(index_name) if index_name else "")
        if query is None:
            raise ValueError("None query is not valid")
        if not isinstance(query, IndexQuery):
            raise ValueError("query must be IndexQuery type")
        if query.query:
            path += "&query={0}".format(Utils.quote_key(query.query))
        if options is None:
            options = BulkOperationOption()
        if not isinstance(options, BulkOperationOption):
            raise ValueError("options must be BulkOperationOption type")

        path += "&pageSize={0}&allowStale={1}&details={2}".format(query.page_size, options.allow_stale,
                                                                  options.retrieve_details)
        if options.max_ops_per_sec:
            path += "&maxOpsPerSec={0}".format(options.max_ops_per_sec)
        if options.stale_timeout:
            path += "&staleTimeout={0}".format(options.stale_timeout)

        return path

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
        args, __, keywords, defaults = inspect.getargspec(entity_init)
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
    def to_lucene(value, action):
        query_text = ""
        if action == "in":
            if not value or len(value) == 0:
                return None
            query_text += "("
            first = True
            for item in value:
                if isinstance(item, str) and ' ' in item:
                    item = "\"{0}\"".format(item)
                item = item
                if first:
                    query_text += "{0}".format(item)
                    first = False
                else:
                    query_text += ",{0}".format(item)
            query_text += ")"
            return query_text

        elif action == "between":
            query_text = "{{{0} TO {1}}}".format(
                Utils.numeric_to_lucene_syntax(value[0]) if value[0] is not None else "*",
                Utils.numeric_to_lucene_syntax(value[1]) if value[1] is not None else "NULL")
        elif action == "equal_between":
            query_text = "[{0} TO {1}]".format(
                Utils.numeric_to_lucene_syntax(value[0]) if value[0] is not None else "*",
                Utils.numeric_to_lucene_syntax(value[1]) if value[1] is not None else "NULL")
        elif action == "search":
            query_text = "({0})".format(value)
        else:
            query_text = value
            if value is None:
                query_text = "[[NULL_VALUE]]"

            if isinstance(value, str) and ' ' in value:
                query_text = "\"{0}\"".format(value)

        return query_text

    @staticmethod
    def numeric_to_lucene_syntax(value):
        if value is None:
            value = "[[NULL_VALUE]]"
        elif not value:
            value = "[[EMPTY_STRING]]"

        python_version = sys.version_info.major

        if (python_version > 2 and value > sys.maxsize and isinstance(value, int)) \
                or python_version <= 2 and isinstance(value, long):
            value = "Lx{0}".format(int(value))

        elif isinstance(value, float) or isinstance(value, int):
            value = "Dx{0}".format(value)

        return value

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
    def string_to_datetime(datetime_str):
        try:
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
        builder = ""
        if length >= 2 and term[0] == '/' and term[1] == '/':
            builder += "//"
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
                    builder += term[start:i - start]

                builder += '\\{0}'.format(ch)
                start = i + 1
                i += 1
                continue

            if ch == ' ' or ch == '\t':
                if make_phrase:
                    return "\"{0}\"".format(Utils.escape(term, allow_wild_cards, False))

            i += 1
        if length > start:
            builder += term[start: length]

        return builder
