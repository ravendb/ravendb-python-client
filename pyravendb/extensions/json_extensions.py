from pyravendb import constants
from pyravendb.data.document_conventions import DocumentConventions
from pyravendb.data.query import IndexQuery
from pyravendb.store.entity_to_json import EntityToJson


class JsonExtensions:
    @staticmethod
    def try_get_conflict(metadata: dict) -> bool:
        if constants.Documents.Metadata.CONFLICT in metadata:
            return bool(metadata.get(constants.Documents.Metadata.CONFLICT))
        return False

    @staticmethod
    def write_index_query(conventions: DocumentConventions, query: IndexQuery) -> dict:
        return {
            "Query": query.query,
            "PageSize": query.page_size,
            "WaitForNonStaleResults": query.wait_for_non_stale_results,
            "Start": query.start,
            "WaitForNonStaleResultsTimeout": query.wait_for_non_stale_results_timeout,
            "DisableCaching": query.disable_caching,
            "SkipDuplicateChecking": query.skip_duplicate_checking,
            "QueryParameters": EntityToJson.convert_entity_to_json_static(query.query_parameters, conventions, None)
            if query.query_parameters
            else None,
            "ProjectionBehavior": query.projection_behavior,
        }
