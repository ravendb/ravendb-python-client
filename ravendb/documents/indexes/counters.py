from typing import Dict, Set, Optional, Callable

from ravendb.primitives import constants
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.indexes.definitions import (
    FieldStorage,
    FieldIndexing,
    FieldTermVector,
    IndexDefinition,
    IndexSourceType,
    IndexFieldOptions,
    IndexType,
)
from ravendb.documents.indexes.abstract_index_creation_tasks import (
    AbstractIndexCreationTaskBase,
    AbstractIndexDefinitionBuilder,
)
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

        return index_definition_builder.to_index_definition(self.conventions)


class AbstractJavaScriptCountersIndexCreationTask(AbstractIndexCreationTaskBase[CountersIndexDefinition]):
    def __init__(self):
        super(AbstractJavaScriptCountersIndexCreationTask, self).__init__()
        self._definition = CountersIndexDefinition()

    @property
    def maps(self) -> Set[str]:
        return self._definition.maps

    @maps.setter
    def maps(self, value: Set[str]):
        self._definition.maps = value

    @property
    def fields(self) -> Dict[str, IndexFieldOptions]:
        return self._definition.fields

    @fields.setter
    def fields(self, value: Dict[str, IndexFieldOptions]):
        self._definition.fields = value

    @property
    def reduce(self) -> str:
        return self._definition.reduce

    @reduce.setter
    def reduce(self, value: Dict[str, IndexFieldOptions]):
        self._definition.reduce = value

    @property
    def is_map_reduce(self) -> bool:
        return self.reduce is not None

    @property
    def output_reduce_to_collection(self) -> str:
        return self._definition.output_reduce_to_collection

    @output_reduce_to_collection.setter
    def output_reduce_to_collection(self, value: Dict[str, IndexFieldOptions]):
        self._definition.output_reduce_to_collection = value

    @property
    def pattern_references_collection_name(self) -> str:
        return self._definition.pattern_references_collection_name

    @pattern_references_collection_name.setter
    def pattern_references_collection_name(self, value: Dict[str, IndexFieldOptions]):
        self._definition.pattern_references_collection_name = value

    @property
    def pattern_for_output_reduce_to_collection_references(self) -> str:
        return self._definition.pattern_for_output_reduce_to_collection_references

    @pattern_for_output_reduce_to_collection_references.setter
    def pattern_for_output_reduce_to_collection_references(self, value: Dict[str, IndexFieldOptions]):
        self._definition.pattern_for_output_reduce_to_collection_references = value

    def create_index_definition(self) -> CountersIndexDefinition:
        self._definition.name = self.index_name
        self._definition.type = IndexType.JAVA_SCRIPT_MAP_REDUCE if self.is_map_reduce else IndexType.JAVA_SCRIPT_MAP
        self._definition.additional_sources = self.additional_sources or {}
        self._definition.additional_assemblies = self.additional_assemblies or set()
        self._definition.configuration = self.configuration
        self._definition.lock_mode = self.lock_mode
        self._definition.priority = self.priority
        self._definition.state = self.state
        self._definition.deployment_mode = self.deployment_mode
        return self._definition
