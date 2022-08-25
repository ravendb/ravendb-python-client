from __future__ import annotations

import inspect
import threading
from abc import abstractmethod, ABC
from datetime import timedelta, datetime
from enum import Enum
from typing import Dict, List, Tuple, Callable, Union, Optional, Generic, Type

import inflect

from typing import TypeVar
from ravendb.json.metadata_as_dictionary import MetadataAsDictionary
from ravendb import constants
from ravendb.documents.operations.configuration import ClientConfiguration, LoadBalanceBehavior, ReadBalanceBehavior
from ravendb.documents.indexes.definitions import SortOptions
from ravendb.tools.utils import Utils

inflect.def_classical["names"] = False
inflector = inflect.engine()

_T = TypeVar("_T")


class DocumentConventions(object):
    __cached_default_type_collection_names: Dict[type, str] = {}

    def __init__(self):
        self._frozen = False

        # Value constraints
        self.identity_parts_separator = "/"
        self.max_number_of_requests_per_session = 30
        self._max_http_cache_size = 128 * 1024 * 1024
        self.max_length_of_query_using_get_url = 1024 + 512

        # Flags
        self.disable_topology_updates = False
        self.use_optimistic_concurrency = False
        self.throw_if_query_page_size_is_not_set = False
        self._send_application_identifier = True
        self._save_enums_as_integers: Optional[bool] = None

        # Configuration
        self.json_default_method = DocumentConventions.json_default
        self._original_configuration: Optional[ClientConfiguration] = None
        self._should_ignore_entity_changes: Optional[ShouldIgnoreEntityChanges] = None

        # Collections
        self._list_of_registered_id_conventions: List[Tuple[Type, Callable[[str, object], str]]] = []
        self._list_of_query_value_to_object_converters: List[Tuple[Type, ValueForQueryConverter[object]]] = []

        # Utilities
        self.document_id_generator: Optional[Callable[[str, object], str]] = None
        self._find_identity_property = lambda q: q.__name__ == "key"
        self._find_python_class: Optional[Callable[[str, Dict], str]] = None
        self._find_collection_name: Callable[[Type], str] = self.default_get_collection_name
        self._find_python_class_name: Callable[
            [Type], str
        ] = lambda object_type: f"{object_type.__module__}.{object_type.__name__}"
        self._transform_class_collection_name_to_document_id_prefix = (
            lambda collection_name: self.default_transform_collection_name_to_document_id_prefix(collection_name)
        )

        # Timeouts
        self.request_timeout: timedelta = timedelta.min
        self.first_broadcast_attempt_timeout = timedelta(seconds=5)
        self.second_broadcast_attempt_timeout = timedelta(seconds=30)
        self.wait_for_indexes_after_save_changes_timeout = timedelta(seconds=15)
        self.wait_for_replication_after_save_changes_timeout = timedelta(seconds=15)
        self.wait_for_non_stale_results_timeout = timedelta(seconds=15)

        # Balancing
        self._load_balancer_context_seed: Optional[int] = None
        self._load_balance_behavior: Optional[LoadBalanceBehavior] = LoadBalanceBehavior.NONE
        self._read_balance_behavior: Optional[ReadBalanceBehavior] = ReadBalanceBehavior.NONE
        self._load_balancer_per_session_context_selector: Optional[Callable[[str], str]] = None

        # Async
        self._update_from_lock = threading.Lock()

    def freeze(self):
        self._frozen = True

    def is_frozen(self):
        return self._frozen

    def get_python_class_name(self, entity_type: type):
        return self._find_python_class_name(entity_type)

    @property
    def find_collection_name(self) -> Callable[[type], str]:
        return self._find_collection_name

    @find_collection_name.setter
    def find_collection_name(self, value) -> None:
        self.__assert_not_frozen()
        self._find_collection_name = value

    @property
    def find_python_class(self) -> Callable[[str, Dict], Optional[str]]:
        def __default(key: str, doc: Dict) -> Optional[str]:
            metadata = doc.get(constants.Documents.Metadata.KEY)
            if metadata:
                python_type = metadata.get(constants.Documents.Metadata.RAVEN_PYTHON_TYPE)
                return python_type
            return None

        return self._find_python_class or __default

    @find_python_class.setter
    def find_python_class(self, value: Callable[[str, Dict], str]):
        self.__assert_not_frozen()
        self._find_python_class = value

    def get_python_class(self, key: str, document: Dict) -> str:
        return self.find_python_class(key, document)

    @property
    def transform_class_collection_name_to_document_id_prefix(self) -> Callable[[str], str]:
        return self._transform_class_collection_name_to_document_id_prefix

    @transform_class_collection_name_to_document_id_prefix.setter
    def transform_class_collection_name_to_document_id_prefix(self, value: Callable[[str], str]) -> None:
        self.__assert_not_frozen()
        self._transform_class_collection_name_to_document_id_prefix = value

    @property
    def load_balancer_per_session_context_selector(self) -> Callable[[str], str]:
        return self._load_balancer_per_session_context_selector

    @load_balancer_per_session_context_selector.setter
    def load_balancer_per_session_context_selector(self, value: Callable[[str], str]):
        self.load_balancer_per_session_context_selector = value

    @property
    def max_http_cache_size(self) -> int:
        return self._max_http_cache_size

    @max_http_cache_size.setter
    def max_http_cache_size(self, value: int):
        self._max_http_cache_size = value

    @property
    def save_enums_as_integers(self) -> bool:
        return self._save_enums_as_integers

    @save_enums_as_integers.setter
    def save_enums_as_integers(self, value: bool):
        self._save_enums_as_integers = value

    @property
    def find_python_class_name(self) -> Callable[[type], str]:
        return self._find_python_class_name

    @find_python_class_name.setter
    def find_python_class_name(self, value) -> None:
        self.__assert_not_frozen()
        self._find_python_class_name = value

    @property
    def should_ignore_entity_changes(self) -> ShouldIgnoreEntityChanges:
        return self._should_ignore_entity_changes

    @should_ignore_entity_changes.setter
    def should_ignore_entity_changes(self, value: ShouldIgnoreEntityChanges) -> None:
        self.__assert_not_frozen()
        self._should_ignore_entity_changes = value

    @property
    def load_balancer_context_seed(self) -> int:
        return self._load_balancer_context_seed

    @load_balancer_context_seed.setter
    def load_balancer_context_seed(self, value: int):
        self.__assert_not_frozen()
        self._load_balancer_context_seed = value

    @property
    def load_balance_behavior(self):
        return self._load_balance_behavior

    @load_balance_behavior.setter
    def load_balance_behavior(self, value: LoadBalanceBehavior):
        self.__assert_not_frozen()
        self._load_balance_behavior = value

    @property
    def read_balance_behavior(self) -> ReadBalanceBehavior:
        return self._read_balance_behavior

    @read_balance_behavior.setter
    def read_balance_behavior(self, value: ReadBalanceBehavior):
        self.__assert_not_frozen()
        self._read_balance_behavior = value

    @property
    def send_application_identifier(self) -> bool:
        return self._send_application_identifier

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
        elif getattr(o, "to_json", None) and getattr(o.to_json, "__call__", None):
            return o.to_json()
        elif getattr(o, "__dict__", None):
            return o.__dict__
        elif isinstance(o, set):
            return list(o)
        elif isinstance(o, (int, float)):
            return str(o)
        else:
            raise TypeError(repr(o) + " is not JSON serializable (Try add a json default method to convention)")

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
        collection_name = self._find_collection_name(object_type)
        if collection_name:
            return collection_name

        return self.default_get_collection_name(object_type)

    def generate_document_id(self, database_name: str, entity: object) -> str:
        object_type = type(entity)
        for list_of_registered_id_convention in self._list_of_registered_id_conventions:
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
        result = inflector.plural(str(object_type.__name__))  # todo: hilo multidb problems
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
        cloned._list_of_registered_id_conventions = [*self._list_of_registered_id_conventions]
        cloned._frozen = self._frozen
        cloned._should_ignore_entity_changes = self._should_ignore_entity_changes
        cloned._original_configuration = self._original_configuration
        cloned._save_enums_as_integers = self._save_enums_as_integers
        cloned.identity_parts_separator = self.identity_parts_separator
        cloned.disable_topology_updates = self.disable_topology_updates
        cloned._find_identity_property = self._find_identity_property

        cloned.document_id_generator = self.document_id_generator

        cloned._find_collection_name = self._find_collection_name
        cloned._find_python_class_name = self.find_python_class_name

        cloned.use_optimistic_concurrency = self.use_optimistic_concurrency
        cloned.throw_if_query_page_size_is_not_set = self.throw_if_query_page_size_is_not_set
        cloned.max_number_of_requests_per_session = self.max_number_of_requests_per_session

        cloned._read_balance_behavior = self._read_balance_behavior
        cloned._load_balance_behavior = self._load_balance_behavior
        self._max_http_cache_size = self._max_http_cache_size

    def update_from(self, configuration: ClientConfiguration):
        if configuration.disabled and self._original_configuration is None:
            return
        with self._update_from_lock:
            if configuration.disabled and self._original_configuration is not None:
                self.max_number_of_requests_per_session = (
                    self._original_configuration.max_number_of_requests_per_session
                    if self._original_configuration.max_number_of_requests_per_session
                    else self.max_number_of_requests_per_session
                )
                self._read_balance_behavior = (
                    self._original_configuration.read_balance_behavior
                    if self._original_configuration.read_balance_behavior
                    else self._read_balance_behavior
                )
                self.identity_parts_separator = (
                    self._original_configuration.identity_parts_separator
                    if self._original_configuration.identity_parts_separator
                    else self.identity_parts_separator
                )
                self._load_balance_behavior = (
                    self._original_configuration.load_balance_behavior
                    if self._original_configuration.load_balance_behavior
                    else self._load_balance_behavior
                )
                self._load_balancer_context_seed = (
                    self._original_configuration.load_balancer_context_seed
                    if self._original_configuration.load_balancer_context_seed
                    else self._load_balancer_context_seed
                )

                self._original_configuration = None
                return

            if self._original_configuration is None:
                self._original_configuration = ClientConfiguration()
                self._original_configuration.etag = -1
                self._original_configuration.max_number_of_requests_per_session = (
                    self.max_number_of_requests_per_session
                )
                self._original_configuration.__read_balance_behavior = self._read_balance_behavior
                self._original_configuration.identity_parts_separator = self.identity_parts_separator
                self._original_configuration.__load_balance_behavior = self._load_balance_behavior
                self._original_configuration.__load_balancer_context_seed = self._load_balancer_context_seed

            # first not None
            self.max_number_of_requests_per_session = next(
                item
                for item in [
                    configuration.max_number_of_requests_per_session,
                    self._original_configuration.max_number_of_requests_per_session,
                    self.max_number_of_requests_per_session,
                ]
                if item is not None
            )

            self._read_balance_behavior = next(
                item
                for item in [
                    configuration.read_balance_behavior,
                    self._original_configuration.read_balance_behavior,
                    self._read_balance_behavior,
                ]
                if item is not None
            )

            self.identity_parts_separator = next(
                item
                for item in [
                    configuration.identity_parts_separator,
                    self._original_configuration.identity_parts_separator,
                    self.identity_parts_separator,
                ]
                if item is not None
            )

            self._load_balance_behavior = next(
                item
                for item in [
                    configuration.load_balance_behavior,
                    self._original_configuration.load_balance_behavior,
                    self._load_balance_behavior,
                ]
                if item is not None
            )

            self._load_balancer_context_seed = (
                configuration.load_balancer_context_seed
                or self._original_configuration.load_balancer_context_seed
                or self._load_balancer_context_seed
            )

    @staticmethod
    def default_transform_collection_name_to_document_id_prefix(collection_name: str) -> str:
        upper_count = len(list(filter(str.isupper, collection_name)))
        if upper_count <= 1:
            return collection_name.lower()

        # multiple capital letters, so probably something that we want to preserve caps on.
        return collection_name

    def try_convert_value_to_object_for_query(
        self, field_name: str, value: object, for_range: bool
    ) -> Tuple[bool, object]:
        for query_value_converter in self._list_of_query_value_to_object_converters:
            if not isinstance(value, query_value_converter[0]):
                continue

            return query_value_converter[1].try_to_convert_value_for_query(field_name, value, for_range)

        return False, None


class ValueForQueryConverter(Generic[_T]):
    @abstractmethod
    def try_to_convert_value_for_query(self, field_name: str, value: _T, for_range: bool) -> Tuple[bool, object]:
        pass


class ShouldIgnoreEntityChanges(ABC):
    @abstractmethod
    def check(
        self,
        session_operations: "InMemoryDocumentSessionOperations",
        entity: object,
        document_id: str,
    ) -> bool:
        pass
