from abc import ABC
from typing import Optional, Set, Dict, Callable, Union, TypeVar, List

from ravendb import IndexDefinition, IndexSourceType
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.indexes.definitions import (
    IndexPriority,
    IndexLockMode,
    IndexDeploymentMode,
    IndexState,
    FieldStorage,
    FieldIndexing,
    FieldTermVector,
    IndexFieldOptions,
    IndexType,
)
from ravendb.documents.indexes.abstract_index_creation_tasks import (
    AbstractIndexCreationTaskBase,
    AbstractIndexDefinitionBuilder,
)
from ravendb.documents.indexes.spatial.configuration import SpatialOptions, SpatialOptionsFactory
from ravendb.primitives import constants


_T_IndexDefinition = TypeVar("_T_IndexDefinition", bound=IndexDefinition)


class TimeSeriesIndexDefinition(IndexDefinition):
    @property
    def source_type(self) -> IndexSourceType:
        return IndexSourceType.TIME_SERIES


class TimeSeriesIndexDefinitionBuilder(AbstractIndexDefinitionBuilder[TimeSeriesIndexDefinition]):
    def __init__(self, index_name: Optional[str] = None):
        super().__init__(index_name)
        self.map: Optional[str] = None

    def _new_index_definition(self) -> TimeSeriesIndexDefinition:
        return TimeSeriesIndexDefinition()

    def to_index_definition(
        self, conventions: DocumentConventions, validate_map: bool = True
    ) -> TimeSeriesIndexDefinition:
        if self.map is None and validate_map:
            raise RuntimeError(
                f"Map is required to generate an index, "
                f"you cannot create an index without a valid Map property (in index {self._index_name})."
            )
        return super().to_index_definition(conventions, validate_map)

    def _to_index_definition(
        self, index_definition: TimeSeriesIndexDefinition, conventions: DocumentConventions
    ) -> None:
        if map is None:
            return

        index_definition.maps.add(self.map)


class AbstractGenericTimeSeriesIndexCreationTask(AbstractIndexCreationTaskBase[TimeSeriesIndexDefinition], ABC):
    def __init__(
        self,
        conventions: DocumentConventions = None,
        priority: IndexPriority = None,
        lock_mode: IndexLockMode = None,
        deployment_mode: IndexDeploymentMode = None,
        state: IndexState = None,
    ):
        super().__init__(conventions, priority, lock_mode, deployment_mode, state)
        self._reduce: Optional[str] = None
        self.stores_strings: Dict[str, FieldStorage] = {}
        self.indexes_strings: Dict[str, FieldIndexing] = {}
        self.analyzers_strings: Dict[str, str] = {}
        self.index_suggestions: Set[str] = set()
        self.term_vectors_strings: Dict[str, FieldTermVector] = {}
        self.spatial_options_strings: Dict[str, SpatialOptions] = {}

        self._output_reduce_to_collection: Optional[str] = None
        self._pattern_for_output_reduce_to_collection_references: Optional[str] = None
        self._pattern_references_collection_name: Optional[str] = None

    @property
    def is_map_reduce(self) -> bool:
        return self._reduce is not None

    def _index(self, field: str, indexing: FieldIndexing) -> None:
        self.indexes_strings[field] = indexing

    def _spatial(self, field: str, indexing: Callable[[SpatialOptionsFactory], SpatialOptions]) -> None:
        self.spatial_options_strings[field] = indexing(SpatialOptionsFactory())

    def _store_all_fields(self, storage: FieldStorage) -> None:
        self.stores_strings[constants.Documents.Indexing.Fields.ALL_FIELDS] = storage

    def _store(self, field: str, storage: FieldStorage) -> None:
        self.stores_strings[field] = storage

    def _analyze(self, field: str, analyzer: str) -> None:
        self.analyzers_strings[field] = analyzer

    def _term_vector(self, field: str, term_vector: FieldTermVector) -> None:
        self.term_vectors_strings[field] = term_vector

    def _suggestion(self, field: str) -> None:
        self.index_suggestions.add(field)


class AbstractTimeSeriesIndexCreationTask(AbstractGenericTimeSeriesIndexCreationTask):
    def __init__(
        self,
        conventions: DocumentConventions = None,
        priority: IndexPriority = None,
        lock_mode: IndexLockMode = None,
        deployment_mode: IndexDeploymentMode = None,
        state: IndexState = None,
    ):
        super().__init__(conventions, priority, lock_mode, deployment_mode, state)
        self.map: Optional[str] = None

    def create_index_definition(self) -> Union[IndexDefinition, _T_IndexDefinition]:
        if self.conventions is None:
            self.conventions = DocumentConventions()

        index_definition_builder = TimeSeriesIndexDefinitionBuilder(self.index_name)
        index_definition_builder.indexes_strings = self.indexes_strings
        index_definition_builder.analyzers_strings = self.analyzers_strings
        index_definition_builder.map = self.map
        index_definition_builder.reduce = self._reduce
        index_definition_builder.stores_strings = self.stores_strings
        index_definition_builder.suggestions_options = self.index_suggestions
        index_definition_builder.term_vector_strings = self.term_vectors_strings
        index_definition_builder.spatial_indexes_strings = self.spatial_options_strings
        index_definition_builder.output_reduce_to_collection = self._output_reduce_to_collection
        index_definition_builder.pattern_for_output_reduce_to_collection_references = (
            self._pattern_for_output_reduce_to_collection_references
        )
        index_definition_builder.pattern_references_collection_name = self._pattern_references_collection_name
        index_definition_builder.additional_sources = self.additional_sources
        index_definition_builder.additional_assemblies = self.additional_assemblies
        index_definition_builder.configuration = self.configuration
        index_definition_builder.lock_mode = self.lock_mode
        index_definition_builder.priority = self.priority
        index_definition_builder.state = self.state
        index_definition_builder.deployment_mode = self.deployment_mode

        return index_definition_builder.to_index_definition(self.conventions)


class AbstractMultiMapTimeSeriesIndexCreationTask(AbstractGenericTimeSeriesIndexCreationTask):
    def __init__(
        self,
        conventions: DocumentConventions = None,
        priority: IndexPriority = None,
        lock_mode: IndexLockMode = None,
        deployment_mode: IndexDeploymentMode = None,
        state: IndexState = None,
    ):
        super().__init__(conventions, priority, lock_mode, deployment_mode, state)
        self.maps: List[str] = []

    def _add_map(self, map: str) -> None:
        if map is None:
            raise ValueError("Map cannot be None.")
        self.maps.append(map)

    def create_index_definition(self) -> Union[IndexDefinition, _T_IndexDefinition]:
        if self.conventions is None:
            self.conventions = DocumentConventions()

        index_definition_builder = TimeSeriesIndexDefinitionBuilder(self.index_name)
        index_definition_builder.indexes_strings = self.indexes_strings
        index_definition_builder.analyzers_strings = self.analyzers_strings
        index_definition_builder.reduce = self._reduce
        index_definition_builder.stores_strings = self.stores_strings
        index_definition_builder.suggestions_options = self.index_suggestions
        index_definition_builder.term_vector_strings = self.term_vectors_strings
        index_definition_builder.spatial_indexes_strings = self.spatial_options_strings
        index_definition_builder.output_reduce_to_collection = self._output_reduce_to_collection
        index_definition_builder.pattern_for_output_reduce_to_collection_references = (
            self._pattern_for_output_reduce_to_collection_references
        )
        index_definition_builder.pattern_references_collection_name = self._pattern_references_collection_name
        index_definition_builder.additional_sources = self.additional_sources
        index_definition_builder.additional_assemblies = self.additional_assemblies
        index_definition_builder.configuration = self.configuration
        index_definition_builder.lock_mode = self.lock_mode
        index_definition_builder.priority = self.priority
        index_definition_builder.state = self.state
        index_definition_builder.deployment_mode = self.deployment_mode

        index_definition = index_definition_builder.to_index_definition(self.conventions, False)
        index_definition.maps = set(self.maps)

        return index_definition


class AbstractJavaScriptTimeSeriesIndexCreationTask(AbstractIndexCreationTaskBase[TimeSeriesIndexDefinition]):
    def __init__(
        self,
        conventions: DocumentConventions = None,
        priority: IndexPriority = None,
        lock_mode: IndexLockMode = None,
        deployment_mode: IndexDeploymentMode = None,
        state: IndexState = None,
    ):
        super().__init__(conventions, priority, lock_mode, deployment_mode, state)
        self._definition = TimeSeriesIndexDefinition()

    @property
    def maps(self) -> Set[str]:
        return self._definition.maps

    @maps.setter
    def maps(self, maps: Set[str]):
        self._definition.maps = maps

    @property
    def fields(self) -> Dict[str, IndexFieldOptions]:
        return self._definition.fields

    @fields.setter
    def fields(self, fields: Dict[str, IndexFieldOptions]):
        self._definition.fields = fields

    @property
    def reduce(self) -> str:
        return self._definition.reduce

    @reduce.setter
    def reduce(self, reduce: str):
        self._definition.reduce = reduce

    def is_map_reduce(self) -> bool:
        return self.reduce is not None

    @property
    def output_reduce_to_collection(self) -> str:
        return self._definition.output_reduce_to_collection

    @output_reduce_to_collection.setter
    def output_reduce_to_collection(self, output_reduce_to_collection: str):
        self._definition.output_reduce_to_collection = output_reduce_to_collection

    @property
    def pattern_references_collection_name(self) -> str:
        return self._definition.pattern_references_collection_name

    @pattern_references_collection_name.setter
    def pattern_references_collection_name(self, pattern_references_collection_name: str):
        self._definition.pattern_references_collection_name = pattern_references_collection_name

    @property
    def pattern_for_output_reduce_to_collection_references(self) -> str:
        return self._definition.pattern_for_output_reduce_to_collection_references

    @pattern_for_output_reduce_to_collection_references.setter
    def pattern_for_output_reduce_to_collection_references(self, pattern_for_output_reduce_to_collection_references):
        self._definition.pattern_for_output_reduce_to_collection_references = (
            pattern_for_output_reduce_to_collection_references
        )

    def create_index_definition(self) -> TimeSeriesIndexDefinition:
        self._definition.name = self.index_name
        self._definition.type = IndexType.JAVA_SCRIPT_MAP_REDUCE if self.is_map_reduce() else IndexType.JAVA_SCRIPT_MAP
        self._definition.additional_sources = self.additional_sources or {}
        self._definition.additional_assemblies = self.additional_assemblies or set()
        self._definition.configuration = self.configuration
        self._definition.lock_mode = self.lock_mode
        self._definition.priority = self.priority
        self._definition.state = self.state
        self._definition.deployment_mode = self.deployment_mode
        return self._definition
