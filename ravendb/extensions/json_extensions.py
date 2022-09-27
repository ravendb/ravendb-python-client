import datetime
import enum
import json
from typing import Dict

from ravendb import constants
from ravendb.documents.conventions import DocumentConventions
from ravendb.documents.queries.index_query import IndexQuery
from ravendb.documents.queries.query import ProjectionBehavior
from ravendb.tools.utils import Utils


class JsonExtensions:
    @staticmethod
    def try_get_conflict(metadata: dict) -> bool:
        if constants.Documents.Metadata.CONFLICT in metadata:
            return bool(metadata.get(constants.Documents.Metadata.CONFLICT))
        return False

    @staticmethod
    def write_index_query(conventions: DocumentConventions, query: IndexQuery) -> dict:
        generator: Dict[str, object] = {"Query": query.query}
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
            generator["QueryParameters"] = JsonExtensions.params_deep_convert_to_json(
                query.query_parameters, conventions
            )
        else:
            generator["QueryParameters"] = None
        if query.projection_behavior is not None and query.projection_behavior is not ProjectionBehavior.DEFAULT:
            generator["ProjectionBehavior"] = query.projection_behavior.value
        return generator

    @staticmethod
    def params_deep_convert_to_json(
        query_parameters: Dict[str, object], conventions: DocumentConventions
    ) -> Dict[str, object]:
        """
        Converts params to serializable (by json.dump) json dictionary.
        If there's custom to_json() method defined inside the parameter class, it is used to do so (mainly used to
        fix keys casing).
        Collections and dicts are converted deeply.
        """
        updated_params = {}
        for name, value in query_parameters.items():
            updated_params[name] = JsonExtensions._convert_parameter_to_json(value, conventions)
        return updated_params

    @staticmethod
    def _convert_parameter_to_json(param: object, conventions: DocumentConventions) -> object:
        if isinstance(param, list):
            param: list
            updated = []
            for item in param:
                updated.append(JsonExtensions._convert_parameter_to_json(item, conventions))
            return updated

        elif isinstance(param, set):
            param: set
            updated = set()
            for item in param:
                updated.add(JsonExtensions._convert_parameter_to_json(item, conventions))
            return updated

        elif isinstance(param, dict):
            param: dict
            updated = {}
            for key, value in param.items():
                updated.update({key: JsonExtensions._convert_parameter_to_json(value, conventions)})
            return updated

        elif isinstance(param, tuple):
            param: tuple
            updated = []
            for i in range(len(param)):
                updated.append(JsonExtensions.params_deep_convert_to_json(param[i], conventions))
            return tuple(updated)

        # todo: create typing protocol (Python 3.8) with to_json inside and typehint the param when it has to_json
        elif constants.json_serialize_method_name in param.__class__.__dict__:
            return param.__class__.__dict__.get(constants.json_serialize_method_name)(param)
        else:
            return param

    @staticmethod
    def write_value_as_bytes(value: object) -> bytes:
        return (
            json.dumps(value.to_json())
            if getattr(value, "to_json", None) and getattr(value.to_json, "__call__", None)
            else json.dumps(value)
        ).encode("utf-8")
