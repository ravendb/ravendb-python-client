from data.operations import BulkOperationOption
from data.indexes import IndexQuery
from custom_exceptions import exceptions
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
    def quote_key(key):
        reserved = '%:=&?~#+!$,;\'@()*[]'
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
        try:
            mod = __import__(components[0])
            for comp in components[1:]:
                mod = getattr(mod, comp)
            return mod
        except (ImportError, ValueError):
            pass
        return None

    @staticmethod
    def is_inherit(parent, child):
        if parent == child:
            return True
        if child is None:
            return False
        if parent != child:
            return Utils.is_inherit(parent, child.__base__)

    @staticmethod
    def convert_to_entity(document, object_type, conventions):
        metadata = document.pop("@metadata")
        original_metadata = metadata.copy()
        type_from_metadata = conventions.try_get_type_from_metadata(metadata)
        object_from_metadata = None
        if type_from_metadata is not None:
            object_from_metadata = Utils.import_class(type_from_metadata)
        entity = _DynamicStructure(**document)
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
        entity_initialize_dict = {}
        args, varargs, keywords, defaults = inspect.getargspec(entity.__class__.__init__)
        if (len(args) - 1) != len(document):
            remainder = len(args)
            if defaults:
                remainder -= len(defaults)
            for i in range(1, remainder):
                entity_initialize_dict[args[i]] = document.get(args[i], None)
            for i in range(remainder, len(args)):
                entity_initialize_dict[args[i]] = document.get(args[i], defaults[i - remainder])
        entity.__init__(**entity_initialize_dict)
        return entity, metadata, original_metadata
