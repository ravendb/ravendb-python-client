from typing import Dict, Set, Optional, Callable, Union

from ravendb import constants
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.indexes.definitions import (
    FieldStorage,
    FieldIndexing,
    FieldTermVector,
    IndexDefinition,
    IndexSourceType,
)
from ravendb.documents.indexes.index_creation import AbstractIndexCreationTaskBase, AbstractIndexDefinitionBuilder
from ravendb.documents.indexes.spatial.configuration import SpatialOptions, SpatialOptionsFactory


class CountersIndexDefinition(IndexDefinition):
    @property
    def source_type(self) -> IndexSourceType:
        return IndexSourceType.COUNTERS


class AbstractGenericCountersIndexCreationTask(AbstractIndexCreationTaskBase[CountersIndexDefinition]):
    def __init__(self):
        super(AbstractGenericCountersIndexCreationTask, self).__init__()
        self._reduce: Optional[str] = None

        self._stores_strings: Dict[str, FieldStorage] = {}
        self._indexes_strings: Dict[str, FieldIndexing] = {}
        self._analyzers_strings: Dict[str, str] = {}
        self._index_suggestions: Set[str] = set()
        self._term_vectors_strings: Dict[str, FieldTermVector] = {}
        self._spatial_options_strings: Dict[str, SpatialOptions] = {}

        self._output_reduce_to_collection: Optional[str] = None
        self._pattern_for_output_reduce_to_collection_references: Optional[str] = None
        self._pattern_references_collection_name: Optional[str] = None

    @property
    def is_map_reduce(self) -> bool:
        return self._reduce is not None

    def _index(self, field: str, indexing: FieldIndexing) -> None:
        self._indexes_strings[field] = indexing

    def _spatial(self, field: str, indexing: Callable[[SpatialOptionsFactory], SpatialOptions]) -> None:
        self._spatial_options_strings[field] = indexing(SpatialOptionsFactory())

    def _store_all_fields(self, storage: FieldStorage) -> None:
        self._stores_strings[constants.Documents.Indexing.Fields.ALL_FIELDS] = storage

    def _store(self, field: str, storage: FieldStorage) -> None:
        self._stores_strings[field] = storage

    def _analyze(self, field: str, analyzer: str) -> None:
        self._analyzers_strings[field] = analyzer

    def _term_vector(self, field: str, term_vector: FieldTermVector) -> None:
        self._term_vectors_strings[field] = term_vector

    def _suggestion(self, field: str) -> None:
        self._index_suggestions.add(field)


class CountersIndexDefinitionBuilder(AbstractIndexDefinitionBuilder[CountersIndexDefinition]):
    def __init__(self, index_name: Optional[str] = None):
        super(CountersIndexDefinitionBuilder, self).__init__(index_name)
        self.map: Optional[str] = None

    def _new_index_definition(self) -> CountersIndexDefinition:
        return CountersIndexDefinition()

    def to_index_definition(
        self, conventions: DocumentConventions, validate_map: bool = True
    ) -> CountersIndexDefinition:
        if self.map is None and validate_map:
            raise RuntimeError(
                f"Map is required to generate an index, you "
                f"cannot create an index without a valid map property (in index {self._index_name})."
            )

        return super(CountersIndexDefinitionBuilder, self).to_index_definition(conventions, validate_map)

    def _to_index_definition(self, index_definition: CountersIndexDefinition, conventions: DocumentConventions) -> None:
        if self.map is None:
            return

        index_definition.maps.add(self.map)


class AbstractCountersIndexCreationTask(AbstractGenericCountersIndexCreationTask):
    def __init__(self):
        super(AbstractCountersIndexCreationTask, self).__init__()
        self.map: Optional[str] = None

    def create_index_definition(self) -> CountersIndexDefinition:
        if self.conventions is None:
            self.conventions = DocumentConventions()

        index_definition_builder = CountersIndexDefinitionBuilder(self.index_name)
        index_definition_builder.indexes_strings = self._indexes_strings
        index_definition_builder.analyzers_strings = self._analyzers_strings
        index_definition_builder.map = self.map
        index_definition_builder.reduce = self._reduce
        index_definition_builder.stores_strings = self._stores_strings
        index_definition_builder.suggestions_options = self._index_suggestions
        index_definition_builder.term_vectors_strings = self._term_vectors_strings
        index_definition_builder.spatial_indexes_strings = self._spatial_options_strings
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
