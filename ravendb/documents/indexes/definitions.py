from __future__ import annotations
import datetime
from enum import Enum
from abc import ABC
from typing import Union, Optional, List, Dict, Set, Iterable
from ravendb.documents.indexes.spatial.configuration import SpatialOptions, AutoSpatialOptions
from ravendb.tools.utils import Utils


# The sort options to use for a particular field
class SortOptions(Enum):
    # No sort options
    none = "None"
    # Sort using term values as Strings. Sort values are str and lower values are at the front.
    str = "String"
    # Sort using term values as encoded Doubles and Longs.
    # Sort values are float or longs and lower values are at the front.
    numeric = "Numeric"

    def __str__(self):
        return self.value


class IndexLockMode(Enum):
    UNLOCK = "Unlock"
    LOCKED_IGNORE = "LockedIgnore"
    LOCKED_ERROR = "LockedError"
    SIDE_BY_SIDE = "SideBySide"

    def __str__(self):
        return self.value


class IndexState(Enum):
    NORMAL = "Normal"
    DISABLED = "Disabled"
    IDLE = "Idle"
    ERROR = "Error"

    def __str__(self):
        return self.value


class IndexPriority(Enum):
    LOW = "Low"
    NORMAL = "Normal"
    HIGH = "High"

    def __str__(self):
        return self.value


class IndexDeploymentMode(Enum):
    PARALLEL = "Parallel"
    ROLLING = "Rolling"

    def __str__(self):
        return self.value


class FieldStorage(Enum):
    YES = "Yes"
    NO = "No"


class FieldIndexing(Enum):
    NO = "No"
    SEARCH = "Search"
    EXACT = "Exact"
    HIGHLIGHTING = "Highlighting"
    DEFAULT = "Default"

    def __str__(self):
        return self.value


class AutoFieldIndexing(Enum):
    NO = "No"
    SEARCH = "Search"
    EXACT = "Exact"
    HIGHLIGHTING = "Highlighting"
    DEFAULT = "Default"

    def __str__(self):
        return self.value


class FieldTermVector(Enum):
    YES = "Yes"
    NO = "No"
    WITH_POSITIONS = "WithPositions"
    WITH_OFFSETS = "WithOffsets"
    WITH_POSITIONS_AND_OFFSETS = "WithPositionsAndOffsets"

    def __str__(self):
        return self.value


class IndexSourceType(Enum):
    NONE = "None"
    DOCUMENTS = "Documents"
    TIME_SERIES = "TimeSeries"
    COUNTERS = "Counters"

    def __str__(self):
        return self.value


class IndexType(Enum):
    NONE = "None"
    AUTO_MAP = "AutoMap"
    AUTO_MAP_REDUCE = "AutoMapReduce"
    MAP = "Map"
    MAP_REDUCE = "MapReduce"
    FAULTY = "Faulty"
    JAVA_SCRIPT_MAP = "JavaScriptMap"
    JAVA_SCRIPT_MAP_REDUCE = "JavaScriptMapReduce"

    def __str__(self):
        return self.value


class AggregationOperation(Enum):
    NONE = "None"
    COUNT = "Count"
    SUM = "Sum"

    def __str__(self):
        return self.value


class GroupByArrayBehavior(Enum):
    NOT_APPLICABLE = "NotApplicable"
    BY_CONTENT = "ByContent"
    BY_INDIVIDUAL_VALUES = "ByIndividualValues"


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

    def to_json(self):
        return {
            "Storage": self.storage,
            "Indexing": self.indexing,
            "TermVector": self.term_vector,
            "Spatial": self.spatial.to_json() if self.spatial else None,
            "Analyzer": self.analyzer,
            "Suggestions": self.suggestions,
        }


class IndexDefinition:
    def __init__(self, **kwargs):
        self.name: Union[None, str] = None
        self.priority: Union[None, IndexPriority] = None
        self.state: Union[None, IndexState] = None
        self.lock_mode: Union[None, IndexLockMode] = None
        self.additional_sources: Dict[str, str] = {}
        self.additional_assemblies: Set[AdditionalAssembly] = set()
        self.__maps: Union[None, Set[str]] = None
        self.fields: Union[None, Dict[str, IndexFieldOptions]] = {}
        self.reduce: Union[None, str] = None
        self.configuration: Dict[str, str] = {}
        self.__index_source_type: Union[None, IndexSourceType] = None
        self.__index_type: Union[None, IndexType] = None
        self.output_reduce_to_collection: Union[None, str] = None
        self.reduce_output_index: Union[None, int] = None
        self.pattern_for_output_reduce_to_collection_references: Union[None, str] = None
        self.pattern_references_collection_name: Union[None, str] = None
        self.deployment_mode: Union[None, IndexDeploymentMode] = None
        self.__dict__.update(kwargs)

    @classmethod
    def from_json(cls, json_dict: dict) -> IndexDefinition:
        result = cls()
        result.name = json_dict["Name"]
        result.priority = IndexPriority(json_dict["Priority"])
        result.state = IndexState(json_dict["State"])
        result.lock_mode = IndexLockMode(json_dict["LockMode"])
        result.additional_sources = json_dict["AdditionalSources"]
        result.additional_assemblies = set(map(AdditionalAssembly, json_dict["AdditionalAssemblies"]))
        result.maps = json_dict["Maps"]
        result.fields = map(
            lambda key, value: {key: IndexFieldOptions(value)}, json_dict["Fields"]
        )  # todo: do also nested types taken in IndexFieldOptions init
        result.reduce = json_dict["Reduce"]
        result.configuration = json_dict["Configuration"]
        source_type = json_dict.get("IndexSourceType", None)
        if source_type is not None:
            result.__index_source_type = IndexSourceType(source_type)
        index_type = json_dict.get("IndexType", None)
        if index_type is not None:
            result.__index_type = IndexType(index_type)
        result.output_reduce_to_collection = json_dict["OutputReduceToCollection"]
        result.reduce_output_index = json_dict["ReduceOutputIndex"]
        result.pattern_for_output_reduce_to_collection_references = json_dict[
            "PatternForOutputReduceToCollectionReferences"
        ]
        result.pattern_references_collection_name = json_dict["PatternReferencesCollectionName"]
        deploy = json_dict.get("DeploymentMode", None)
        if deploy is not None:
            result.deployment_mode = IndexDeploymentMode(deploy)
        return result

    def to_json(self) -> dict:
        return {
            "Name": self.name,
            "Priority": self.priority,
            "State": self.state,
            "LockMode": self.lock_mode,
            "AdditionalSources": self.additional_sources,
            "AdditionalAssemblies": [x.to_json() for x in self.additional_assemblies]
            if self.additional_assemblies
            else None,
            "Maps": self.maps,
            "Fields": {key: value.to_json() for key, value in self.fields.items()},
            "Reduce": self.reduce,
            "Configuration": self.configuration,
            "IndexSourceType": self.__index_source_type,
            "IndexType": self.__index_type,
            "OutputReduceToCollection": self.output_reduce_to_collection,
            "ReduceOutputIndex": self.reduce_output_index,
            "PatternForOutputReduceToCollectionReferences": self.pattern_for_output_reduce_to_collection_references,
            "PatternReferencesCollectionName": self.pattern_references_collection_name,
            "DeploymentMode": self.deployment_mode,
        }

    @property
    def maps(self):
        if self.__maps is None:
            self.__maps = set()
        return self.__maps

    @maps.setter
    def maps(self, value: Union[str, Iterable[str]]):
        if isinstance(value, str):
            self.__maps = {value}
        else:
            self.__maps = set(value)

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
        map_fields: Optional[Dict[str, AutoIndexFieldOptions]] = None,
        group_by_fields: Optional[Dict[str, AutoIndexFieldOptions]] = None,
    ):
        self.type = type
        self.name = name
        self.priority = priority
        self.state = state
        self.collection = collection
        self.map_fields = map_fields
        self.group_by_fields = group_by_fields

    @classmethod
    def from_json(cls, json_dict: Dict) -> AutoIndexDefinition:
        return cls(
            IndexType(json_dict.get("Type")),
            json_dict.get("Name"),
            IndexPriority(json_dict.get("Priority")),
            IndexState(json_dict.get("State")) if json_dict.get("State", None) else None,
            json_dict.get("Collection"),
            {name: AutoIndexFieldOptions.from_json(value) for name, value in json_dict.get("MapFields").items()},
            {name: AutoIndexFieldOptions.from_json(value) for name, value in json_dict.get("GroupByFields").items()},
        )

    def to_json(self) -> Dict:
        return {
            "Type": self.type.value,
            "Name": self.name,
            "Priority": self.priority.value,
            "State": self.state.value if self.state is not None else None,
            "Collection": self.collection,
            "MapFields": {key: map_field.to_json() for key, map_field in self.map_fields.items()},
            "GroupByFields": {key: group_by_field.to_json() for key, group_by_field in self.group_by_fields.items()},
        }


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

    @classmethod
    def from_json(cls, json_dict: Dict) -> AutoIndexFieldOptions:
        return cls(
            FieldStorage(json_dict.get("Storage")),
            AutoFieldIndexing(json_dict.get("Indexing")),
            AggregationOperation(json_dict.get("Aggregation")) if json_dict.get("Aggregation", None) else None,
            AutoSpatialOptions.from_json(json_dict.get("Spatial")) if json_dict.get("Spatial", None) else None,
            GroupByArrayBehavior(json_dict.get("GroupByArrayBehavior")),
            json_dict.get("Suggestions"),
            json_dict.get("IsNameQuoted"),
        )

    def to_json(self) -> Dict:
        return {
            "Storage": self.storage.value,
            "Indexing": self.indexing.value,
            "Aggregation": self.aggregation.value if self.aggregation is not None else None,
            "Spatial": self.spatial.type if self.spatial is not None else None,
            "GroupByArrayBehavior": self.group_by_array_behavior.value,
            "Suggestions": self.suggestions,
            "IsNameQuoted": self.is_name_quoted,
        }


class AdditionalAssembly:
    def __init__(
        self,
        assembly_name: Optional[str] = None,
        assembly_path: Optional[str] = None,
        package_name: Optional[str] = None,
        package_version: Optional[str] = None,
        package_source_url: Optional[str] = None,
        usings: Optional[Set[str]] = None,
    ):
        self.assembly_name = assembly_name
        self.assembly_path = assembly_path
        self.package_name = package_name
        self.package_version = package_version
        self.package_source_url = package_source_url
        self.usings = usings

    def to_json(self) -> dict:
        return {
            "AssemblyName": self.assembly_name,
            "AssemblyPath": self.assembly_path,
            "PackageName": self.package_name,
            "PackageVersion": self.package_version,
            "PackageSourceUrl": self.package_source_url,
            "Usings": self.usings,
        }

    @classmethod
    def only_usings(cls, usings: Set[str]) -> AdditionalAssembly:
        if not usings:
            raise ValueError("using cannot be None or empty")
        return cls(usings=usings)

    @classmethod
    def from_runtime(cls, assembly_name: str, usings: Optional[Set[str]] = None) -> AdditionalAssembly:
        if not assembly_name or assembly_name.isspace():
            raise ValueError("Assembly name cannot be None or whitespace")
        return cls(assembly_name, usings=usings)

    @classmethod
    def from_path(cls, assembly_path: str, usings: Optional[Set[str]] = None) -> AdditionalAssembly:
        if not assembly_path or assembly_path.isspace():
            raise ValueError("Assembly path cannot be None or whitespace")
        return cls(assembly_path=assembly_path, usings=usings)

    @classmethod
    def from_NuGet(
        cls,
        package_name: str,
        package_version: str,
        package_source_url: Optional[str] = None,
        usings: Optional[Set[str]] = None,
    ) -> AdditionalAssembly:
        if not package_name or package_name.isspace():
            raise ValueError("Package name cannot be None or whitespace")
        if not package_version or package_version.isspace():
            raise ValueError("Package version cannot be None or whitespace")

        return cls(
            package_name=package_name,
            package_version=package_version,
            package_source_url=package_source_url,
            usings=usings,
        )


class AbstractCommonApiForIndexes(ABC):
    def __init__(self):
        self.__additional_sources: Union[None, Dict[str, str]] = None
        self.__additional_assemblies: Union[None, Set[AdditionalAssembly]] = None
        self.__configuration: Union[None, Dict[str, str]] = None

    @property
    def is_map_reduce(self) -> bool:
        return False

    @property
    def index_name(self) -> str:
        return type(self).__name__.replace("_", "/")

    @property
    def additional_sources(self) -> Dict[str, str]:
        return self.__additional_sources

    @additional_sources.setter
    def additional_sources(self, value: Dict[str, str]):
        self.__additional_sources = value

    @property
    def additional_assemblies(self):
        return self.__additional_assemblies

    @additional_assemblies.setter
    def additional_assemblies(self, value: Set[AdditionalAssembly]):
        self.__additional_assemblies = value

    @property
    def configuration(self) -> Dict[str, str]:
        return self.__configuration

    @configuration.setter
    def configuration(self, value: Dict[str, str]):
        self.__configuration = value


class RollingIndexState(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    DONE = "DONE"

    def __str__(self):
        return self.value


class RollingIndex:
    def __init__(self, active_deployments: Optional[Dict[str, RollingIndexDeployment]]):
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

    @classmethod
    def from_json(cls, json_dict: dict) -> IndexingError:
        return cls(
            json_dict["Error"],
            Utils.string_to_datetime(json_dict["Timestamp"]),
            json_dict["Document"],
            json_dict["Action"],
        )


class IndexErrors:
    def __init__(self, name: Optional[str] = None, errors: Optional[List[IndexingError]] = None):
        self.name = name
        self.errors = errors

    @classmethod
    def from_json(cls, json_dict: dict) -> IndexErrors:
        return cls(json_dict["Name"], list(map(lambda x: IndexingError.from_json(x), json_dict["Errors"])))
