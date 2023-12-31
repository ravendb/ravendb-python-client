import logging
from typing import Collection, TYPE_CHECKING, Optional, List, TypeVar

from ravendb.documents.indexes.definitions import IndexDefinition, IndexPriority, IndexState
from ravendb.documents.operations.indexes import PutIndexesOperation

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore
    from ravendb.documents.conventions import DocumentConventions
    from ravendb.documents.indexes.abstract_index_creation_tasks import AbstractIndexCreationTask


class IndexCreation:
    @staticmethod
    def create_indexes(
        indexes: Collection["AbstractIndexCreationTask"],
        store: "DocumentStore",
        conventions: Optional["DocumentConventions"] = None,
    ) -> None:
        if conventions is None:
            conventions = store.conventions

        try:
            indexes_to_add = IndexCreation.create_indexes_to_add(indexes, conventions)
            store.maintenance.send(PutIndexesOperation(*indexes_to_add))
        except Exception as e:
            logging.info("Could not create indexes in one shot (maybe using older version of RavenDB ?)", exc_info=e)
            for index in indexes:
                index.execute(store, conventions)

    @staticmethod
    def create_indexes_to_add(
        index_creation_tasks: "AbstractIndexCreationTask", conventions: "DocumentConventions"
    ) -> List[IndexDefinition]:
        def __map(x: "AbstractIndexCreationTask"):
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
