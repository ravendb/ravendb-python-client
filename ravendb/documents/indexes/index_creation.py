import logging
from abc import abstractmethod, ABC
from typing import Generic, TypeVar, Union, Dict, Set, Callable, Optional, List, Collection

from ravendb import constants
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.indexes.spatial.configuration import SpatialOptionsFactory
from ravendb.documents.store.definition import DocumentStore, DocumentStoreBase
from ravendb.documents.indexes.definitions import (
    IndexDefinition,
    AbstractCommonApiForIndexes,
    IndexPriority,
    IndexLockMode,
    IndexDeploymentMode,
    IndexState,
    FieldIndexing,
    FieldStorage,
    FieldTermVector,
    AdditionalAssembly,
    SpatialOptions,
    IndexFieldOptions,
    IndexType,
)
from ravendb.documents.operations.indexes import PutIndexesOperation

_T_IndexDefinition = TypeVar("_T_IndexDefinition", bound=IndexDefinition)


class AbstractIndexCreationTaskBase(AbstractCommonApiForIndexes, Generic[_T_IndexDefinition]):
    @abstractmethod
    def create_index_definition(self) -> Union[IndexDefinition, _T_IndexDefinition]:
        pass

    def __init__(
        self,
        conventions: DocumentConventions = None,
        priority: IndexPriority = None,
        lock_mode: IndexLockMode = None,
        deployment_mode: IndexDeploymentMode = None,
        state: IndexState = None,
    ):
        super().__init__()
        self.conventions = conventions
        self.priority = priority
        self.lock_mode = lock_mode
        self.deployment_mode = deployment_mode
        self.state = state

    def execute(self, store: DocumentStore, conventions: DocumentConventions = None, database: str = None):
        old_conventions = self.conventions
        database = DocumentStoreBase.get_effective_database(store, database)
        try:
            self.conventions = conventions or self.conventions or store.get_request_executor(database).conventions
            index_definition = self.create_index_definition()
            index_definition.name = self.index_name

            if self.lock_mode is not None:
                index_definition.lock_mode = self.lock_mode

            if self.priority is not None:
                index_definition.priority = self.priority

            if self.state is not None:
                index_definition.state = self.state

            if self.deployment_mode is not None:
                index_definition.deployment_mode = self.deployment_mode

            store.maintenance.for_database(database).send(PutIndexesOperation(index_definition))

        finally:
            self.conventions = old_conventions


class AbstractGenericIndexCreationTask(
    Generic[_T_IndexDefinition], AbstractIndexCreationTaskBase[_T_IndexDefinition], ABC
):
    def __init__(self):
        super().__init__()

        self._reduce: Union[None, str] = None

        self._stores_strings: Dict[str, FieldStorage] = {}
        self._indexes_strings: Dict[str, FieldIndexing] = {}
        self._analyzers_strings: Dict[str, str] = {}
        self._index_suggestions: Set[str] = set()
        self._term_vectors_strings: Dict[str, FieldTermVector] = {}
        self._spatial_options_strings: Dict[str, SpatialOptions] = {}

        self._output_reduce_to_collection: Union[None, str] = None
        self._pattern_for_output_reduce_to_collection_references: Union[None, str] = None
        self._pattern_references_collection_name: Union[None, str] = None

    @property
    def reduce(self) -> str:
        return self._reduce

    @reduce.setter
    def reduce(self, value: str):
        self._reduce = value

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
        """
        Register a field to be stored
        @param field: Field name
        @param storage: Field storage value to use
        """
        self._stores_strings[field] = storage

    def _analyze(self, field: str, analyzer: str) -> None:
        """
        Register a field to be analyzed
        @param field: Field name
        @param analyzer: analyzer to use
        """
        self._analyzers_strings[field] = analyzer

    def _term_vector(self, field: str, term_vector: FieldTermVector) -> None:
        """
        Register a field to have term vectors
        @param field: Field name
        @param term_vector: TermVector type
        """
        self._term_vectors_strings[field] = term_vector

    def _suggestion(self, field: str) -> None:
        self._index_suggestions.add(field)

    def _add_assembly(self, assembly: AdditionalAssembly) -> None:
        if assembly is None:
            raise ValueError("Assembly cannot be None")

        if self.additional_assemblies is None:
            self.additional_assemblies = set()

        self.additional_assemblies.add(assembly)


class AbstractIndexDefinitionBuilder(Generic[_T_IndexDefinition]):
    def __init__(self, index_name: str):
        self._index_name = index_name or self.__class__.__name__
        if len(self._index_name) > 256:
            raise ValueError(f"The index name is limited to 256 characters, but was: {self._index_name}")

        self.reduce: Union[None, str] = None

        self.stores_strings: Dict[str, FieldStorage] = {}
        self.indexes_strings: Dict[str, FieldIndexing] = {}
        self.analyzers_strings: Dict[str, str] = {}
        self.suggestions_options: Set[str] = set()
        self.term_vectors_strings: Dict[str, FieldTermVector] = {}
        self.spatial_indexes_strings: Dict[str, SpatialOptions] = {}

        self.lock_mode: Union[None, IndexLockMode] = None
        self.priority: Union[None, IndexLockMode] = None
        self.state: Union[None, IndexState] = None
        self.deployment_mode: Union[None, IndexDeploymentMode] = None

        self.output_reduce_to_collection: Union[None, str] = None
        self.pattern_for_output_reduce_to_collection_references: Union[None, str] = None
        self.pattern_references_collection_name: Union[None, str] = None

        self.additional_sources: Union[None, Dict[str, str]] = None
        self.additional_assemblies: Union[None, Set[AdditionalAssembly]] = None
        self.configuration: Dict[str, str] = {}

    @abstractmethod
    def _new_index_definition(self) -> Union[IndexDefinition, _T_IndexDefinition]:
        pass

    @abstractmethod
    def _to_index_definition(self, index_definition: _T_IndexDefinition, conventions: DocumentConventions) -> None:
        pass

    def __apply_values(
        self,
        index_definition: IndexDefinition,
        values: Dict[str, object],
        action: Callable[[IndexFieldOptions, object], None],
    ) -> None:
        for key, value in values.items():
            field = index_definition.fields.get(key, IndexFieldOptions())
            action(field, value)
            index_definition.fields[key] = field  # if the field wasn't indexed yet, we need to set it.

    def to_index_definition(self, conventions: DocumentConventions, validate_map: bool = True) -> _T_IndexDefinition:
        try:
            index_definition = self._new_index_definition()
            index_definition.name = self._index_name
            index_definition.reduce = self.reduce
            index_definition.lock_mode = self.lock_mode
            index_definition.priority = self.priority
            index_definition.state = self.state
            index_definition.output_reduce_to_collection = self.output_reduce_to_collection
            index_definition.pattern_for_output_reduce_to_collection_references = (
                self.pattern_for_output_reduce_to_collection_references
            )
            index_definition.pattern_references_collection_name = self.pattern_references_collection_name

            suggestions = {}
            for suggestions_option in self.suggestions_options:
                suggestions[suggestions_option] = True

            def __set_indexing(options, value):
                options.indexing = value

            def __set_storage(options, value):
                options.storage = value

            def __set_analyzer(options, value):
                options.analyzer = value

            def __set_term_vector(options, value):
                options.term_vector = value

            def __set_spatial(options, value):
                options.spatial = value

            def __set_suggestions(options, value):
                options.suggestions = value

            self.__apply_values(index_definition, self.indexes_strings, __set_indexing)
            self.__apply_values(index_definition, self.stores_strings, __set_storage)
            self.__apply_values(index_definition, self.analyzers_strings, __set_analyzer)
            self.__apply_values(index_definition, self.term_vectors_strings, __set_term_vector)
            self.__apply_values(index_definition, self.spatial_indexes_strings, __set_spatial)
            self.__apply_values(index_definition, suggestions, __set_suggestions)

            index_definition.additional_sources = self.additional_sources
            index_definition.additional_assemblies = self.additional_assemblies
            index_definition.configuration = self.configuration

            self._to_index_definition(index_definition, conventions)

            return index_definition

        except Exception as e:
            raise RuntimeError(f"Failed to create index {self._index_name}", e)  # todo: IndexCompilationException


class IndexDefinitionBuilder(AbstractIndexDefinitionBuilder[IndexDefinition]):
    def __init__(self, index_name: Optional[str] = None):
        super().__init__(index_name)
        self.map: Union[None, str] = None

    def _new_index_definition(self) -> IndexDefinition:
        return IndexDefinition()

    def to_index_definition(self, conventions: DocumentConventions, validate_map: bool = True) -> IndexDefinition:
        if self.map is None and validate_map:
            raise ValueError(
                f"Map is required to generate an index, "
                f"you cannot create an index without a valid map property (in index {self._index_name})."
            )
        return super().to_index_definition(conventions, validate_map)

    def _to_index_definition(self, index_definition: IndexDefinition, conventions: DocumentConventions) -> None:
        if self.map is None:
            return

        index_definition.maps.add(self.map)


class AbstractIndexCreationTask(AbstractGenericIndexCreationTask[IndexDefinition], ABC):
    def __init__(self):
        super().__init__()
        self._map: Union[None, str] = None

    @property
    def map(self) -> str:
        return self._map

    @map.setter
    def map(self, value: str):
        self._map = value

    def create_index_definition(self) -> IndexDefinition:
        if self.conventions is None:
            self.conventions = DocumentConventions()

        index_definition_builder = IndexDefinitionBuilder(self.index_name)
        index_definition_builder.indexes_strings = self._indexes_strings
        index_definition_builder.analyzers_strings = self._analyzers_strings
        index_definition_builder.map = self._map
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


_T_AbstractIndexCreationTask = TypeVar("_T_AbstractIndexCreationTask", bound=AbstractIndexCreationTask)


class AbstractJavaScriptIndexCreationTask(AbstractIndexCreationTaskBase[IndexDefinition]):
    def __init__(self):
        super().__init__()
        self._definition = IndexDefinition()

    @property
    def maps(self) -> List[str]:
        return self._definition.maps

    @maps.setter
    def maps(self, value: List[str]):
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
    def reduce(self, value: str):
        self._definition.reduce = value

    @property
    def output_reduce_to_collection(self) -> str:
        return self._definition.output_reduce_to_collection

    @output_reduce_to_collection.setter
    def output_reduce_to_collection(self, value: str):
        self._definition.output_reduce_to_collection = value

    @property
    def pattern_references_collection_name(self) -> str:
        return self._definition.pattern_references_collection_name

    @pattern_references_collection_name.setter
    def pattern_references_collection_name(self, value: str):
        self._definition.pattern_references_collection_name = value

    @property
    def pattern_for_output_reduce_to_collection_references(self) -> str:
        return self._definition.pattern_for_output_reduce_to_collection_references

    @pattern_for_output_reduce_to_collection_references.setter
    def pattern_for_output_reduce_to_collection_references(self, value: str):
        self._definition.pattern_for_output_reduce_to_collection_references = value

    def create_index_definition(self) -> Union[IndexDefinition, _T_IndexDefinition]:
        self._definition.name = self.index_name
        self._definition.type = IndexType.JAVA_SCRIPT_MAP_REDUCE if self.is_map_reduce else IndexType.JAVA_SCRIPT_MAP
        if self.additional_sources:
            self._definition.additional_sources = self.additional_sources
        else:
            self._definition.additional_sources = {}

        if self.additional_assemblies:
            self._definition.additional_assemblies = self.additional_assemblies
        else:
            self._definition.additional_assemblies = {}

        self._definition.configuration = self.configuration
        self._definition.lock_mode = self.lock_mode
        self._definition.priority = self.priority
        self._definition.state = self.state
        self._definition.deployment_mode = self.deployment_mode
        return self._definition


class AbstractMultiMapIndexCreationTask(AbstractGenericIndexCreationTask[IndexDefinition]):
    def __init__(self):
        super().__init__()
        self.__maps: List[str] = []

    def _add_map(self, map: str) -> None:
        if map is None:
            raise ValueError("Map cannot be None")

        self.__maps.append(map)

    def create_index_definition(self):
        if self.conventions is None:
            self.conventions = DocumentConventions()

        index_definition_builder = IndexDefinitionBuilder(self.index_name)
        index_definition_builder.indexes_strings = self._indexes_strings
        index_definition_builder.analyzers_strings = self._analyzers_strings
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

        index_definition = index_definition_builder.to_index_definition(self.conventions, False)
        index_definition.maps = set(self.__maps)

        return index_definition


class IndexCreation:
    @staticmethod
    def create_indexes(
        indexes: Collection[_T_AbstractIndexCreationTask],
        store: DocumentStore,
        conventions: Optional[DocumentConventions] = None,
    ) -> None:
        if conventions is None:
            conventions = store.conventions

        try:
            indexes_to_add = IndexCreation.create_indexes_to_add(indexes, conventions)
            store.maintenance.send(PutIndexesOperation(indexes_to_add))
        except Exception as e:
            logging.info("Could not create indexes in one shot (maybe using older version of RavenDB ?)", exc_info=e)
            for index in indexes:
                index.execute(store, conventions)

    @staticmethod
    def create_indexes_to_add(
        index_creation_tasks: Collection[_T_AbstractIndexCreationTask], conventions: DocumentConventions
    ) -> List[IndexDefinition]:
        def __map(x: _T_AbstractIndexCreationTask):
            old_conventions = x.conventions
            try:
                x.conventions = conventions
                definition = x.create_index_definition()
                definition.name = x.index_name
                definition.priority = x.priority or IndexPriority.NORMAL
                definition.state = x.state or IndexState.NORMAL
                return definition
            finally:
                x.conventions = old_conventions

        return list(map(__map, index_creation_tasks))
