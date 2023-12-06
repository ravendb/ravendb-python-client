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
)
from ravendb.documents.indexes.index_creation import AbstractIndexCreationTaskBase, AbstractIndexDefinitionBuilder
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
