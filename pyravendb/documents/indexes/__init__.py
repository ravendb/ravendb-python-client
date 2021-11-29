from __future__ import annotations
import datetime
from enum import Enum
from abc import abstractmethod
from typing import Union, Optional
from pyravendb.documents.indexes.spatial import SpatialOptions, AutoSpatialOptions


class IndexLockMode(Enum):
    UNLOCK = "UNLOCK"
    LOCKED_IGNORE = "LOCKED_IGNORE"
    LOCKED_ERROR = "LOCKED_ERROR"
    SIDE_BY_SIDE = "SIDE_BY_SIDE"

    def __str__(self):
        return self.value


class IndexState(Enum):
    NORMAL = "NORMAL"
    DISABLED = "DISABLED"
    IDLE = "IDLE"
    ERROR = "ERROR"

    def __str__(self):
        return self.value


class IndexPriority(Enum):
    LOW = "LOW"
    NORMAL = "NORMAL"
    HIGH = "HIGH"

    def __str__(self):
        return self.value


class IndexDeploymentMode(Enum):
    PARALLEL = "PARALLEL"
    ROLLING = "ROLLING"

    def __str__(self):
        return self.value


class FieldStorage(Enum):
    YES = " YES"
    NO = "NO"


class FieldIndexing(Enum):
    YES = " YES"
    NO = "NO"
    SEARCH = "SEARCH"
    EXACT = "EXACT"
    HIGHLIGHTING = "HIGHLIGHTING"
    DEFAULT = "DEFAULT"

    def __str__(self):
        return self.value


class AutoFieldIndexing(Enum):
    NO = "NO"
    SEARCH = "SEARCH"
    EXACT = "EXACT"
    HIGHLIGHTING = "HIGHLIGHTING"
    DEFAULT = "DEFAULT"

    def __str__(self):
        return self.value


class FieldTermVector(Enum):
    YES = " YES"
    NO = "NO"
    WITH_POSITIONS = "WITH_POSITIONS"
    WITH_OFFSETS = "WITH_OFFSETS"
    WITH_POSITIONS_AND_OFFSETS = "WITH_POSITIONS_AND_OFFSETS"

    def __str__(self):
        return self.value


class IndexSourceType(Enum):
    NONE = "NONE"
    DOCUMENTS = "DOCUMENTS"
    TIME_SERIES = "TIME_SERIES"
    COUNTERS = "COUNTERS"

    def __str__(self):
        return self.value


class IndexType(Enum):
    NONE = "NONE"
    AUTO_MAP = "AUTO_MAP"
    AUTO_MAP_REDUCE = "AUTO_MAP_REDUCE"
    MAP = "MAP"
    MAP_REDUCE = "MAP_REDUCE"
    FAULTY = "FAULTY"
    JAVA_SCRIPT_MAP = "JAVA_SCRIPT_MAP"
    JAVA_SCRIPT_MAP_REDUCE = "JAVA_SCRIPT_MAP_REDUCE"

    def __str__(self):
        return self.value


class AggregationOperation(Enum):
    NONE = "NONE"
    COUNT = "COUNT"
    SUM = "SUM"

    def __str__(self):
        return self.value


class GroupByArrayBehavior(Enum):
    NOT_APPLICABLE = "NOT_APPLICABLE"
    BY_CONTENT = "BY_CONTENT"
    BY_INDIVIDUAL_VALUES = "BY_INDIVIDUAL_VALUES"


class IndexFieldOptions:
    def __init__(
        self,
        storage: Optional[FieldStorage] = None,
        indexing: Optional[FieldIndexing] = None,
        term_vector: Optional[FieldTermVector] = None,
        spatial: Optional[SpatialOptions] = None,
        analyzer: Optional[str] = None,
        suggestions: Optional[bool] = None,
    ):
        self.storage = storage
        self.indexing = indexing
        self.term_vector = term_vector
        self.spatial = spatial
        self.analyzer = analyzer
        self.suggestions = suggestions


class IndexDefinition:
    def __init__(self):
        self.name: Union[None, str] = None
        self.priority: Union[None, IndexPriority] = None
        self.state: Union[None, IndexState] = None
        self.lock_mode: Union[None, IndexLockMode] = None
        self.additional_sources: dict[str, str] = {}
        self.additional_assemblies: set[AdditionalAssembly] = set()
        self.maps: Union[None, set[str]] = None
        self.fields: Union[None, dict[str, IndexFieldOptions]] = {}
        self.reduce: Union[None, str] = None
        self.configuration: dict[str, str] = {}
        self.__index_source_type: Union[None, IndexSourceType] = None
        self.__index_type: Union[None, IndexType] = None
        self.output_reduce_to_collection: Union[None, str] = None
        self.reduce_output_index: Union[None, int] = None
        self.pattern_for_output_reduce_to_collection_references: Union[None, str] = None
        self.pattern_references_collection_name: Union[None, str] = None
        self.deployment_mode: Union[None, IndexDeploymentMode] = None

    @property
    def source_type(self) -> IndexSourceType:
        if self.__index_source_type is None or self.__index_source_type == IndexSourceType.NONE:
            self.__index_source_type = self.detect_static_index_source_type()
        return self.__index_source_type

    @source_type.setter
    def source_type(self, value: IndexSourceType):
        self.__index_source_type = value

    @property
    def type(self) -> IndexType:
        if self.__index_type is None or self.__index_type == IndexType.NONE:
            self.__index_type = self.detect_static_index_type()
        return self.__index_type

    @type.setter
    def type(self, value: IndexType):
        self.__index_type = value

    def detect_static_index_source_type(self) -> IndexSourceType:
        if not self.maps:
            raise ValueError("Index definition contains no maps")

        source_type = IndexSourceType.NONE
        for map in self.maps:
            map_source_type = None  # todo: IndexDefinitionHelper.detect_static_index_source_type(map)
            if source_type == IndexSourceType.NONE:
                source_type = map_source_type
                continue

            if source_type != map_source_type:
                raise RuntimeError("Index definition cannot contain maps with different source types.")

        return source_type

    def detect_static_index_type(self) -> IndexType:
        first_map = self.maps.__iter__().__next__()

        if first_map is None:
            raise ValueError("Index definitions contains no Maps")

        return None  # todo: IndexDefinitionHelper.detect_Static_index_type(first_map, self.reduce)


class AutoIndexDefinition:
    def __init__(
        self,
        type: Optional[IndexType] = None,
        name: Optional[str] = None,
        priority: Optional[IndexPriority] = None,
        state: Optional[IndexState] = None,
        collection: Optional[str] = None,
        map_fields: Optional[dict[str, AutoIndexFieldOptions]] = None,
        group_by_fields: Optional[dict[str, AutoIndexFieldOptions]] = None,
    ):
        self.type = type
        self.name = name
        self.priority = priority
        self.state = state
        self.collection = collection
        self.map_fields = map_fields
        self.group_by_fields = group_by_fields


class AutoIndexFieldOptions:
    def __init__(
        self,
        storage: Optional[FieldStorage] = None,
        indexing: Optional[AutoFieldIndexing] = None,
        aggregation: Optional[AggregationOperation] = None,
        spatial: Optional[AutoSpatialOptions] = None,
        group_by_array_behavior: Optional[GroupByArrayBehavior] = None,
        suggestions: Optional[bool] = None,
        is_name_quoted: Optional[bool] = None,
    ):
        self.storage = storage
        self.indexing = indexing
        self.aggregation = aggregation
        self.spatial = spatial
        self.group_by_array_behavior = group_by_array_behavior
        self.suggestions = suggestions
        self.is_name_quoted = is_name_quoted


class AdditionalAssembly:
    def __init__(
        self,
        assembly_name: Optional[str] = None,
        assembly_path: Optional[str] = None,
        package_name: Optional[str] = None,
        package_version: Optional[str] = None,
        package_source_url: Optional[str] = None,
        usings: Optional[set[str]] = None,
    ):
        self.assembly_name = assembly_name
        self.assembly_path = assembly_path
        self.package_name = package_name
        self.package_version = package_version
        self.package_source = package_source_url
        self.usings = usings

    @staticmethod
    def only_usings(usings: set[str]) -> AdditionalAssembly:
        if not usings:
            raise ValueError("using cannot be None or empty")
        return AdditionalAssembly(usings=usings)

    @staticmethod
    def from_runtime(assembly_name: str, usings: Optional[set[str]] = None) -> AdditionalAssembly:
        if not assembly_name or assembly_name.isspace():
            raise ValueError("Assembly name cannot be None or whitespace")
        return AdditionalAssembly(assembly_name, usings=usings)

    @staticmethod
    def from_path(assembly_path: str, usings: Optional[set[str]] = None) -> AdditionalAssembly:
        if not assembly_path or assembly_path.isspace():
            raise ValueError("Assembly path cannot be None or whitespace")
        return AdditionalAssembly(assembly_path=assembly_path, usings=usings)

    @staticmethod
    def from_nu_get(
        package_name: str,
        package_version: str,
        package_source_url: Optional[str] = None,
        usings: Optional[set[str]] = None,
    ) -> AdditionalAssembly:
        if not package_name or package_name.isspace():
            raise ValueError("Package name cannot be None or whitespace")
        if not package_version or package_version.isspace():
            raise ValueError("Package version cannot be None or whitespace")

        return AdditionalAssembly(
            package_name=package_name,
            package_version=package_version,
            package_source_url=package_source_url,
            usings=usings,
        )


class AbstractCommonApiForIndexes:
    @abstractmethod
    def __init__(self):
        self.__additional_sources: Union[None, dict[str, str]] = None
        self.__additional_assemblies: Union[None, set[AdditionalAssembly]] = None
        self.__configuration: Union[None, dict[str, str]] = None

    @property
    def is_map_reduce(self) -> bool:
        return False

    @property
    def index_name(self) -> str:
        return type(self).__name__.replace("_", "/")

    @property
    def additional_sources(self) -> dict[str, str]:
        return self.__additional_sources

    @additional_sources.setter
    def additional_sources(self, value: dict[str, str]):
        self.__additional_sources = value

    @property
    def additional_assemblies(self):
        return self.__additional_assemblies

    @additional_assemblies.setter
    def additional_assemblies(self, value: set[AdditionalAssembly]):
        self.__additional_assemblies = value

    @property
    def configuration(self) -> dict[str, str]:
        return self.__configuration

    @configuration.setter
    def configuration(self, value: dict[str, str]):
        self.__configuration = value

    @staticmethod
    def only_usings(usings: set[str]) -> AdditionalAssembly:
        if not usings:
            raise ValueError("Using cannot be None or empty")
        return AdditionalAssembly(usings=usings)

    @staticmethod
    def from_runtime(assembly_name: str, usings: set[str] = None) -> AdditionalAssembly:
        if not assembly_name or assembly_name.isspace():
            raise ValueError("assembly_name cannot be None or whitespace.")
        return AdditionalAssembly(assembly_name=assembly_name, usings=usings)

    @staticmethod
    def from_path(assembly_path: str, usings: set[str] = None) -> AdditionalAssembly:
        if not assembly_path or assembly_path.isspace():
            raise ValueError("assembly_path cannot be None or whitespace")
        return AdditionalAssembly(assembly_path=assembly_path, usings=usings)

    @staticmethod
    def from_nuget(
        package_name: str, package_version: str, package_source_url: str = None, usings: set[str] = None
    ) -> AdditionalAssembly:
        if not package_name or package_name.isspace():
            raise ValueError("package_name cannot be None or whitespace")
        if not package_version or package_version.isspace():
            raise ValueError("package_version cannot be None or whitespace")

        return AdditionalAssembly(
            package_name=package_name,
            package_version=package_version,
            package_source_url=package_source_url,
            usings=usings,
        )


class RollingIndexState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"

    def __str__(self):
        return self.value


class RollingIndex:
    def __init__(self, active_deployments: Optional[dict[str, RollingIndexDeployment]]):
        self.active_deployments = active_deployments


class RollingIndexDeployment:
    def __init__(
        self,
        state: Optional[RollingIndexState] = None,
        created_at: Optional[datetime.datetime] = None,
        started_at: Optional[datetime.datetime] = None,
        finished_at: Optional[datetime.datetime] = None,
    ):
        self.state = state
        self.created_at = created_at
        self.started_at = started_at
        self.finished_at = finished_at


class IndexingError:
    def __init__(
        self,
        error: Optional[str] = None,
        timestamp: Optional[datetime.datetime] = None,
        document: Optional[str] = None,
        action: Optional[str] = None,
    ):
        self.error = error
        self.timestamp = timestamp
        self.document = document
        self.action = action

    def __str__(self):
        return f"Error: {self.error}, Document: {self.document}, Action: {self.action}"


class IndexErrors:
    def __init__(self, name: Optional[str] = None, errors: Optional[list[IndexingError]] = None):
        self.name = name
        self.errors = errors
