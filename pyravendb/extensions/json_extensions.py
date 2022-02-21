from pyravendb import constants
from pyravendb.documents.conventions.document_conventions import DocumentConventions
from pyravendb.documents.queries.index_query import IndexQuery
from pyravendb.documents.queries.query import ProjectionBehavior
from pyravendb.documents.session.entity_to_json import EntityToJson


class JsonExtensions:
    @staticmethod
    def try_get_conflict(metadata: dict) -> bool:
        if constants.Documents.Metadata.CONFLICT in metadata:
            return bool(metadata.get(constants.Documents.Metadata.CONFLICT))
        return False

    @staticmethod
    def write_index_query(conventions: DocumentConventions, query: IndexQuery) -> dict:
        generator = {"Query": query.query}
        if query.is_page_size_set and query.page_size >= 0:
            generator["PageSize"] = query.page_size
        if query.wait_for_non_stale_results:
            generator["WaitForNonStaleResults"] = query.wait_for_non_stale_results

        if query.start is not None and query.start > 0:
            generator["Start"] = query.start

        if query.wait_for_non_stale_results_timeout is not None:
            generator["WaitForNonStaleResultsTimeout"] = query.wait_for_non_stale_results_timeout

        if query.disable_caching:
            generator["DisableCaching"] = query.disable_caching

        if query.skip_duplicate_checking:
            generator["SkipDuplicateChecking"] = query.skip_duplicate_checking

        if query.query_parameters is not None:
            generator["QueryParameters"] = EntityToJson.convert_entity_to_json_static(
                query.query_parameters, conventions, None
            )
        else:
            generator["QueryParameters"] = None
        if query.projection_behavior is not None and query.projection_behavior is not ProjectionBehavior.DEFAULT:
            generator["ProjectionBehavior"] = query.projection_behavior.value
        return generator
