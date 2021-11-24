from __future__ import annotations

import threading
from typing import Union, Callable, TYPE_CHECKING

import pyravendb.json.metadata_as_dictionary
from pyravendb.data.indexes import SortOptions
from pyravendb.custom_exceptions.exceptions import InvalidOperationException
from datetime import datetime, timedelta

from pyravendb.documents.operations.configuration import ClientConfiguration
from pyravendb.http import ReadBalanceBehavior, LoadBalanceBehavior
from pyravendb.tools.utils import Utils
from enum import Enum
import inflect
import inspect


inflect.def_classical["names"] = False
inflector = inflect.engine()

if TYPE_CHECKING:
    from pyravendb.documents.conventions import ShouldIgnoreEntityChanges


class DocumentConventions(object):
    __cached_default_type_collection_names: dict[type, str] = {}

    def __init__(self, **kwargs):
        self.max_number_of_requests_per_session = kwargs.get("max_number_of_request_per_session", 30)
        self.max_ids_to_catch = kwargs.get("max_ids_to_catch", 32)
        # timeout for wait to server in seconds
        self.timeout = kwargs.get("timeout", timedelta(seconds=30))
        self.use_optimistic_concurrency = kwargs.get("use_optimistic_concurrency", False)
        self.json_default_method = DocumentConventions._json_default
        self.max_length_of_query_using_get_url = kwargs.get("max_length_of_query_using_get_url", 1024 + 512)
        self.identity_parts_separator = "/"
        self.disable_topology_updates = kwargs.get("disable_topology_update", False)
        # If set to 'true' then it will throw an exception when any query is performed (in session)
        # without explicit page size set
        self.raise_if_query_page_size_is_not_set = kwargs.get("raise_if_query_page_size_is_not_set", False)
        # To be able to map objects. give mappers a dictionary
        # with the object type that you want to map and a function(key, value) key will be the name of the property
        # the mappers will be used in json.decode
        self._mappers = kwargs.get("mappers", {})

        self.__list_of_registered_id_conventions: list[tuple[type, Callable[[str, object], str]]] = []

        self._frozen = False
        self.__original_configuration: Union[None, ClientConfiguration] = None
        self.__save_enums_as_integers: Union[None, bool] = None

        self.__transform_class_collection_name_to_document_id_prefix = (
            lambda collection_name: self.default_transform_collection_name_to_document_id_prefix(collection_name)
        )
        self.document_id_generator: Union[None, Callable[[str, object], str]] = None
        self.__load_balancer_per_session_context_selector: Union[None, Callable[[str], str]] = None

        self.__find_collection_name: Callable[[type], str] = kwargs.get(
            "find_collection_name", self.default_get_collection_name
        )
        self.__find_python_class_name: Callable[[type], str] = kwargs.get(
            "find_python_class_name", lambda object_type: f"{object_type.__module__}.{object_type.__name__}"
        )
        self.__should_ignore_entity_changes: Union[None, ShouldIgnoreEntityChanges] = None

        self.throw_if_query_page_size_is_not_set: bool = False
        self.__find_identity_property = lambda q: q.__name__ == "key"

        # Timeouts
        self.request_timeout: timedelta = timedelta.min
        self.first_broadcast_attempt_timeout = timedelta(seconds=5)
        self.second_broadcast_attempt_timeout = timedelta(seconds=30)
        self.wait_for_indexes_after_save_changes_timeout = timedelta(seconds=15)
        self.wait_for_replication_after_save_changes_timeout = timedelta(seconds=15)
        self.wait_for_non_stale_results_timeout = timedelta(seconds=15)

        self.__load_balancer_context_seed: Union[None, int] = None
        self.__load_balance_behavior: Union[None, LoadBalanceBehavior] = LoadBalanceBehavior.NONE
        self.__read_balance_behavior: Union[None, ReadBalanceBehavior] = ReadBalanceBehavior.NONE
        self.__max_http_cache_size = 128 * 1024 * 1024

        self.__send_application_identifier = True

        self.__update_from_lock = threading.Lock()

    def freeze(self):
        self._frozen = True

    def is_frozen(self):
        return self._frozen

    def get_python_class_name(self, entity_type: type):
        return self.__find_python_class_name(entity_type)

    @property
    def find_collection_name(self) -> Callable[[type], str]:
        return self.__find_collection_name

    @find_collection_name.setter
    def find_collection_name(self, value) -> None:
        self.__assert_not_frozen()
        self.__find_collection_name = value

    @property
    def transform_class_collection_name_to_document_id_prefix(self) -> Callable[[str], str]:
        return self.__transform_class_collection_name_to_document_id_prefix

    @transform_class_collection_name_to_document_id_prefix.setter
    def transform_class_collection_name_to_document_id_prefix(self, value: Callable[[str], str]) -> None:
        self.__assert_not_frozen()
        self.__transform_class_collection_name_to_document_id_prefix = value

    @property
    def load_balancer_per_session_context_selector(self) -> Callable[[str], str]:
        return self.__load_balancer_per_session_context_selector

    @load_balancer_per_session_context_selector.setter
    def load_balancer_per_session_context_selector(self, value: Callable[[str], str]):
        self.load_balancer_per_session_context_selector = value

    @property
    def max_http_cache_size(self) -> int:
        return self.__max_http_cache_size

    @max_http_cache_size.setter
    def max_http_cache_size(self, value: int):
        self.__max_http_cache_size = value

    @property
    def save_enums_as_integers(self) -> bool:
        return self.__save_enums_as_integers

    @save_enums_as_integers.setter
    def save_enums_as_integers(self, value: bool):
        self.__save_enums_as_integers = value

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
    def load_balancer_context_seed(self) -> int:
        return self.__load_balancer_context_seed

    @load_balancer_context_seed.setter
    def load_balancer_context_seed(self, value: int):
        self.__assert_not_frozen()
        self.__load_balancer_context_seed = value

    @property
    def load_balance_behavior(self):
        return self.__load_balance_behavior

    @load_balance_behavior.setter
    def load_balance_behavior(self, value: LoadBalanceBehavior):
        self.__assert_not_frozen()
        self.__load_balance_behavior = value

    @property
    def read_balance_behavior(self) -> ReadBalanceBehavior:
        return self.__read_balance_behavior

    @read_balance_behavior.setter
    def read_balance_behavior(self, value: ReadBalanceBehavior):
        self.__assert_not_frozen()
        self.__read_balance_behavior = value

    @property
    def send_application_identifier(self) -> bool:
        return self.__send_application_identifier

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
        result = inflector.plural(str(object_type.__name__))
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

    def clone(self) -> DocumentConventions:
        cloned = DocumentConventions()
        cloned.__list_of_registered_id_conventions = [*self.__list_of_registered_id_conventions]
        cloned._frozen = self._frozen
        cloned.__should_ignore_entity_changes = self.__should_ignore_entity_changes
        cloned.__original_configuration = self.__original_configuration
        cloned.__save_enums_as_integers = self.__save_enums_as_integers
        cloned.identity_parts_separator = self.identity_parts_separator
        cloned.disable_topology_updates = self.disable_topology_updates
        cloned.__find_identity_property = self.__find_identity_property

        cloned.document_id_generator = self.document_id_generator

        cloned.__find_collection_name = self.__find_collection_name
        cloned.__find_python_class_name = self.find_python_class_name

        cloned.use_optimistic_concurrency = self.use_optimistic_concurrency
        cloned.throw_if_query_page_size_is_not_set = self.throw_if_query_page_size_is_not_set
        cloned.max_number_of_requests_per_session = self.max_number_of_requests_per_session

        cloned.__read_balance_behavior = self.__read_balance_behavior
        cloned.__load_balance_behavior = self.__load_balance_behavior
        self.__max_http_cache_size = self.__max_http_cache_size

    def update_from(self, configuration: ClientConfiguration):
        if configuration.disabled and self.__original_configuration is None:
            return
        with self.__update_from_lock:
            if configuration.disabled and self.__original_configuration is not None:
                self.max_number_of_requests_per_session = (
                    self.__original_configuration.max_number_of_requests_per_session
                    if self.__original_configuration.max_number_of_requests_per_session
                    else self.max_number_of_requests_per_session
                )
                self.__read_balance_behavior = (
                    self.__original_configuration.read_balance_behavior
                    if self.__original_configuration.read_balance_behavior
                    else self.__read_balance_behavior
                )
                self.identity_parts_separator = (
                    self.__original_configuration.identity_parts_separator
                    if self.__original_configuration.identity_parts_separator
                    else self.identity_parts_separator
                )
                self.__load_balance_behavior = (
                    self.__original_configuration.load_balance_behavior
                    if self.__original_configuration.load_balance_behavior
                    else self.__load_balance_behavior
                )
                self.__load_balancer_context_seed = (
                    self.__original_configuration.load_balancer_context_seed
                    if self.__original_configuration.load_balancer_context_seed
                    else self.__load_balancer_context_seed
                )

                self.__original_configuration = None
                return

            if self.__original_configuration is None:
                self.__original_configuration = ClientConfiguration()
                self.__original_configuration.etag = -1
                self.__original_configuration.max_number_of_requests_per_session = (
                    self.max_number_of_requests_per_session
                )
                self.__original_configuration.__read_balance_behavior = self.__read_balance_behavior
                self.__original_configuration.identity_parts_separator = self.identity_parts_separator
                self.__original_configuration.__load_balance_behavior = self.__load_balance_behavior
                self.__original_configuration.__load_balancer_context_seed = self.__load_balancer_context_seed

            self.max_number_of_requests_per_session = next(
                item
                for item in [
                    configuration.max_number_of_requests_per_session,
                    self.__original_configuration.max_number_of_requests_per_session,
                    self.max_number_of_requests_per_session,
                ]
                if item is not None
            )

            self.__read_balance_behavior = next(
                item
                for item in [
                    configuration.read_balance_behavior,
                    self.__original_configuration.read_balance_behavior,
                    self.__read_balance_behavior,
                ]
                if item is not None
            )

            self.identity_parts_separator = next(
                item
                for item in [
                    configuration.identity_parts_separator,
                    self.__original_configuration.identity_parts_separator,
                    self.identity_parts_separator,
                ]
                if item is not None
            )

            self.__load_balance_behavior = next(
                item
                for item in [
                    configuration.load_balance_behavior,
                    self.__original_configuration.load_balance_behavior,
                    self.__load_balance_behavior,
                ]
                if item is not None
            )

            self.__load_balancer_context_seed = next(
                item
                for item in [
                    configuration.load_balancer_context_seed,
                    self.__original_configuration.load_balancer_context_seed,
                    self.__load_balancer_context_seed,
                ]
                if item is not None
            )

    @staticmethod
    def default_transform_collection_name_to_document_id_prefix(collection_name: str) -> str:
        upper_count = len(list(filter(lambda char: char.isupper, collection_name)))
        if upper_count <= 1:
            return collection_name.lower()

        # multiple capital letters, so probably something that we want to preserve caps on.
        return collection_name
