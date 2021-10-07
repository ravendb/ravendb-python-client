from typing import Union, Callable

import pyravendb.json.metadata_as_dictionary
from pyravendb.data.indexes import SortOptions
from pyravendb.custom_exceptions.exceptions import InvalidOperationException
from datetime import datetime, timedelta

from pyravendb.documents.conventions.conventions import ShouldIgnoreEntityChanges
from pyravendb.tools.utils import Utils
from enum import Enum
import inflect
import inspect


inflect.def_classical["names"] = False
inflector = inflect.engine()


class DocumentConventions(object):
    __cached_default_type_collection_names: dict[type, str] = {}

    def __init__(self, **kwargs):
        self.max_number_of_request_per_session = kwargs.get("max_number_of_request_per_session", 30)
        self.max_ids_to_catch = kwargs.get("max_ids_to_catch", 32)
        # timeout for wait to server in seconds
        self.timeout = kwargs.get("timeout", timedelta(seconds=30))
        self.use_optimistic_concurrency = kwargs.get("use_optimistic_concurrency", False)
        self.json_default_method = DocumentConventions._json_default
        self.max_length_of_query_using_get_url = kwargs.get("max_length_of_query_using_get_url", 1024 + 512)
        self.identity_parts_separator = "/"
        self.disable_topology_update = kwargs.get("disable_topology_update", False)
        # If set to 'true' then it will throw an exception when any query is performed (in session)
        # without explicit page size set
        self.raise_if_query_page_size_is_not_set = kwargs.get("raise_if_query_page_size_is_not_set", False)
        # To be able to map objects. give mappers a dictionary
        # with the object type that you want to map and a function(key, value) key will be the name of the property
        # the mappers will be used in json.decode
        self._mappers = kwargs.get("mappers", {})

        self.__list_of_registered_id_conventions: list[tuple[type, Callable[[str, object], str]]] = []

        self._frozen = False

        self.document_id_generator: Union[None, Callable[[str, object], str]] = None

        self.__find_collection_name: Callable[[type], str] = kwargs.get(
            "find_collection_name", self.default_get_collection_name
        )
        self.__find_python_class_name: Callable[[type], str] = kwargs.get(
            "find_python_class_name", lambda object_type: f"{object_type.__module__}.{object_type.__name__}"
        )
        self.__should_ignore_entity_changes: Union[None, ShouldIgnoreEntityChanges] = None

        self.throw_if_query_page_size_is_not_set: bool = False

        # Timeouts
        self.request_timeout: timedelta = timedelta.min
        self.first_broadcast_attempt_timeout = timedelta(seconds=5)
        self.second_broadcast_attempt_timeout = timedelta(seconds=30)
        self.wait_for_indexes_after_save_changes_timeout = timedelta(seconds=15)
        self.wait_for_replication_after_save_changes_timeout = timedelta(seconds=15)
        self.wait_for_non_stale_results_timeout = timedelta(seconds=15)

        self.__send_application_identifier = True

    def freeze(self):
        self._frozen = True

    def is_frozen(self):
        return self._frozen

    @property
    def find_collection_name(self) -> Callable[[type], str]:
        return self.__find_collection_name

    @find_collection_name.setter
    def find_collection_name(self, value) -> None:
        self.__assert_not_frozen()
        self.__find_collection_name = value

    @property
    def find_python_class_name(self) -> Callable[[type], str]:
        return self.__find_python_class_name

    @find_python_class_name.setter
    def find_python_class_name(self, value) -> None:
        self.__assert_not_frozen()
        self.__find_python_class_name = value

    @property
    def should_ignore_entity_changes(self) -> ShouldIgnoreEntityChanges:
        return self.__should_ignore_entity_changes

    @should_ignore_entity_changes.setter
    def should_ignore_entity_changes(self, value: ShouldIgnoreEntityChanges) -> None:
        self.__assert_not_frozen()
        self.__should_ignore_entity_changes = value

    @property
    def mappers(self):
        return self._mappers

    def update_mappers(self, mapper):
        if self._frozen:
            raise InvalidOperationException(
                "Conventions has frozen after store.initialize() and no changes can be applied to them"
            )
        self._mappers.update(mapper)

    @staticmethod
    def _json_default(o):
        if o is None:
            return None

        if isinstance(o, datetime):
            return Utils.datetime_to_string(o)
        elif isinstance(o, timedelta):
            return Utils.timedelta_to_str(o)
        elif isinstance(o, Enum):
            return o.value
        elif isinstance(o, pyravendb.json.metadata_as_dictionary.MetadataAsDictionary):
            return o.metadata
        elif getattr(o, "__dict__", None):
            return o.__dict__
        elif isinstance(o, set):
            return list(o)
        elif isinstance(o, int) or isinstance(o, float):
            return str(o)
        else:
            raise TypeError(repr(o) + " is not JSON serializable (Try add a json default method to store convention)")

    @staticmethod
    def default_transform_plural(name):
        return inflector.plural(name)

    @staticmethod
    def default_transform_type_tag_name(name):
        count = sum(1 for c in name if c.isupper())
        if count <= 1:
            return DocumentConventions.default_transform_plural(name.lower())
        return DocumentConventions.default_transform_plural(name)

    @staticmethod
    def build_default_metadata(entity):
        if entity is None:
            return {}
        existing = entity.__dict__.get("@metadata")
        if existing is None:
            existing = {}

        new_metadata = {
            "@collection": DocumentConventions.default_transform_plural(entity.__class__.__name__),
            "Raven-Python-Type": "{0}.{1}".format(entity.__class__.__module__, entity.__class__.__name__),
        }

        existing.update(new_metadata)

        return existing

    def get_collection_name(self, entity_or_type: Union[type, object]) -> str:
        if not entity_or_type:
            return None
        object_type = type(entity_or_type) if not isinstance(entity_or_type, type) else entity_or_type
        collection_name = self.__find_collection_name(object_type)
        if collection_name:
            return collection_name

        return self.default_get_collection_name(object_type)

    def generate_document_id(self, database_name: str, entity: object) -> str:
        object_type = type(entity)
        for list_of_registered_id_convention in self.__list_of_registered_id_conventions:
            return list_of_registered_id_convention[1](database_name, entity)
        return self.document_id_generator(database_name, entity)

    @staticmethod
    def default_get_collection_name(object_type: type) -> str:
        result = DocumentConventions.__cached_default_type_collection_names.get(object_type)
        if result:
            return result
        # we want to reject queries and other operations on abstract types, because you usually
        # want to use them for polymorphic queries, and that require the conventions to be
        # applied properly, so we reject the behavior and hint to the user explicitly
        if inspect.isabstract(object_type):
            raise ValueError(
                f"Cannot find collection name for abstract class {object_type}, "
                f"only concrete class are supported. "
                f"Did you forget to customize conventions.find_collection_name?"
            )
        result = inflector.plural(str(object_type))
        DocumentConventions.__cached_default_type_collection_names[object_type] = result
        return result

    @staticmethod
    def try_get_type_from_metadata(metadata):
        if "Raven-Python-Type" in metadata:
            return metadata["Raven-Python-Type"]
        return None

    @staticmethod
    def uses_range_type(obj):
        if obj is None:
            return False

        if isinstance(obj, int) or isinstance(obj, float):
            return True
        return False

    @staticmethod
    def get_default_sort_option(type_name):
        if not type_name:
            return None
        if type_name == "int" or type_name == "float" or type_name == "long":
            return SortOptions.numeric

    @staticmethod
    def range_field_name(field_name, type_name):
        if type_name == "long":
            field_name = "{0}_L_Range".format(field_name)
        else:
            field_name = "{0}_D_Range".format(field_name)

        return field_name

    def __assert_not_frozen(self) -> None:
        if self._frozen:
            raise RuntimeError(
                "Conventions has been frozen after documentStore.initialize()" " and no changes can be applied to them"
            )
