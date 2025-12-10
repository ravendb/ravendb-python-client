from __future__ import annotations
import re
from typing import TYPE_CHECKING, Dict, Any, List, Optional

if TYPE_CHECKING:
    from ravendb.documents.operations.etl.etl_type import EtlType


class CountersTransformation:
    """
    Internal class for counters transformation validation.
    """

    LOAD = "loadCounter"
    ADD = "addCounter"
    MARKER = "$counter/"

    _ADD_METHOD_REGEX = re.compile(ADD)
    _LOAD_BEHAVIOR_METHOD_REGEX = re.compile(r"function\s+loadCountersOf(\w+)Behavior\s*\(.+\)")
    _LOAD_BEHAVIOR_METHOD_NAME_REGEX = re.compile(r"loadCountersOf(\w+)Behavior")

    def __init__(self, parent: "Transformation"):
        self._parent = parent
        self.is_adding_counters: bool = False
        self.collection_to_load_behavior_function: Optional[Dict[str, str]] = None

    def validate(self, errors: List[str], etl_type: "EtlType") -> None:
        from ravendb.documents.operations.etl.etl_type import EtlType

        script = self._parent.script or ""
        self.is_adding_counters = len(self._ADD_METHOD_REGEX.findall(script)) > 0

        if self.is_adding_counters and etl_type == EtlType.SQL:
            errors.append("Adding counters isn't supported by SQL ETL")

        self._fill_collection_to_load_counter_behavior_function(errors, etl_type)

    def _fill_collection_to_load_counter_behavior_function(self, errors: List[str], etl_type: "EtlType") -> None:
        from ravendb.documents.operations.etl.etl_type import EtlType

        script = self._parent.script or ""
        counter_behaviors = list(self._LOAD_BEHAVIOR_METHOD_REGEX.finditer(script))
        if not counter_behaviors:
            return

        if etl_type == EtlType.SQL:
            errors.append("Load counter behavior functions aren't supported by SQL ETL")
            return

        self.collection_to_load_behavior_function = {}
        for match in counter_behaviors:
            if len(match.groups()) != 1:
                errors.append(
                    "Invalid load counters behavior function. It is expected to have the following signature: "
                    "loadCountersOf<CollectionName>Behavior(docId, counterName) and return 'true' if counter should be loaded to a destination"
                )
                continue

            function_signature = match.group(0)
            collection = match.group(1)

            function_name_match = self._LOAD_BEHAVIOR_METHOD_NAME_REGEX.search(function_signature)
            function_name = function_name_match.group(0) if function_name_match else ""

            if collection not in self._parent.collections:
                script_collections = ", ".join(f"'{c}'" for c in self._parent.collections)
                errors.append(
                    f"There is '{function_name}' function defined in '{self._parent.name}' script while the processed collections "
                    f"({script_collections}) doesn't include '{collection}'. "
                    "loadCountersOf<CollectionName>Behavior() function is meant to be defined only for counters of docs from collections that "
                    "are loaded to the same collection on a destination side"
                )
            else:
                collections_from_script = self._parent.get_collections_from_script()
                if collections_from_script and collection not in collections_from_script:
                    errors.append(
                        f"`{function_name}` function where Defined while there is not load to {collection}. "
                        "Load behavior function apply only if load to default collection"
                    )

            if collection in self.collection_to_load_behavior_function:
                errors.append(f"There are multiple '{function_name}' functions defined")

            self.collection_to_load_behavior_function[collection] = function_name


class TimeSeriesTransformation:
    """
    Internal class for time series transformation validation.
    """

    MARKER = "$timeSeries/"
    ADD_TIME_SERIES_NAME = "addTimeSeries"
    ADD_TIME_SERIES_SIGNATURE = "addTimeSeries(timeSeriesReference)"
    LOAD_TIME_SERIES_NAME = "loadTimeSeries"
    LOAD_TIME_SERIES_SIGNATURE = "loadTimeSeries(timeSeriesName, from, to)"
    HAS_TIME_SERIES_NAME = "hasTimeSeries"
    GET_TIME_SERIES_NAME = "getTimeSeries"
    LOAD_TIME_SERIES_BEHAVIOR_SIGNATURE = "loadTimeSeriesOf<CollectionName>Behavior(docId, timeSeriesName)"
    LOAD_TIME_SERIES_BEHAVIOR_PARAMS_COUNT = 2

    _ADD_TIME_SERIES_REGEX = re.compile(ADD_TIME_SERIES_NAME)
    _LOAD_TIME_SERIES_BEHAVIOR_REGEX = re.compile(
        r"function\s+(?P<func_name>loadTimeSeriesOf(?P<collection>[A-Za-z]\w*)Behavior)\s*\(\s*"
        r"((?P<param>[a-zA-Z]\w*)\s*(?:,\s*(?P<param2>[a-zA-Z]\w*)\s*)*)?\s*\)"
    )

    def __init__(self, parent: "Transformation"):
        self._parent = parent
        self.is_adding_time_series: bool = False
        self.collection_to_load_behavior_function: Optional[Dict[str, str]] = None

    def validate(self, errors: List[str], etl_type: "EtlType") -> None:
        from ravendb.documents.operations.etl.etl_type import EtlType

        script = self._parent.script or ""
        self.is_adding_time_series = len(self._ADD_TIME_SERIES_REGEX.findall(script)) > 0

        if self.is_adding_time_series and etl_type == EtlType.SQL:
            errors.append("Adding time series isn't supported by SQL ETL")

        self._fill_collection_to_load_time_series_behavior_function(errors, etl_type)

    def _fill_collection_to_load_time_series_behavior_function(self, errors: List[str], etl_type: "EtlType") -> None:
        from ravendb.documents.operations.etl.etl_type import EtlType

        script = self._parent.script or ""
        time_series_behaviors = list(self._LOAD_TIME_SERIES_BEHAVIOR_REGEX.finditer(script))
        if not time_series_behaviors:
            return

        if etl_type == EtlType.SQL:
            errors.append("Load time series behavior functions aren't supported by SQL ETL")
            return

        self.collection_to_load_behavior_function = {}
        for match in time_series_behaviors:
            function_name = match.group("func_name")
            collection = match.group("collection")

            # Count params by checking captured groups
            params = [g for g in [match.group("param"), match.group("param2")] if g]
            if len(params) > self.LOAD_TIME_SERIES_BEHAVIOR_PARAMS_COUNT:
                errors.append(
                    f"'{function_name} function defined with {len(params)}. "
                    f"The signature should be {self.LOAD_TIME_SERIES_BEHAVIOR_SIGNATURE}"
                )

            if collection not in self._parent.collections:
                script_collections = ", ".join(f"'{c}'" for c in self._parent.collections)
                errors.append(
                    f"There is '{function_name}' function defined in '{self._parent.name}' script while the processed collections "
                    f"({script_collections}) doesn't include '{collection}'. "
                    f"{self.LOAD_TIME_SERIES_BEHAVIOR_SIGNATURE} function is meant to be defined only for time series of docs from collections that "
                    "are loaded to the same collection on a destination side"
                )
            else:
                collections_from_script = self._parent.get_collections_from_script()
                if collections_from_script and collection not in collections_from_script:
                    errors.append(
                        f"`{function_name}` function where Defined while there is not load to {collection}. "
                        "Load behavior function apply only if load to default collection"
                    )

            if collection in self.collection_to_load_behavior_function:
                errors.append(f"There are multiple '{function_name}' functions defined")

            self.collection_to_load_behavior_function[collection] = function_name


class Transformation:
    """
    Represents an ETL transformation script configuration.
    """

    LOAD_TO = "loadTo"
    LOAD_ATTACHMENT = "loadAttachment"
    ADD_ATTACHMENT = "addAttachment"
    ATTACHMENT_MARKER = "$attachment/"
    GENERIC_DELETE_DOCUMENTS_BEHAVIOR_FUNCTION_KEY = "$deleteDocumentsBehavior<>"
    GENERIC_DELETE_DOCUMENTS_BEHAVIOR_FUNCTION_NAME = "deleteDocumentsBehavior"

    # Regex patterns
    _LOAD_TO_METHOD_REGEX = re.compile(r"loadTo(\w+)")
    _LOAD_TO_METHOD_REGEX_ALT = re.compile(r"loadTo\('([\w.]*)'\)|loadTo\(\"([\w.]*)\"\)")
    _LOAD_ATTACHMENT_METHOD_REGEX = re.compile(LOAD_ATTACHMENT)
    _ADD_ATTACHMENT_METHOD_REGEX = re.compile(ADD_ATTACHMENT)
    _LEGACY_REPLICATE_TO_METHOD_REGEX = re.compile(r"replicateTo(\w+)")

    # Parameters and function body regex for matching JS functions
    _PARAMETERS_AND_FUNCTION_BODY_REGEX = (
        r"\s*\((?:[^)(]+|\((?:[^)(]+|\([^)(]*\))*\))*\)\s*\{(?:[^}{]+|\{(?:[^}{]+|\{[^}{]*\})*\})*\}"
    )
    _DELETE_DOCUMENTS_BEHAVIOR_METHOD_REGEX = re.compile(
        r"function\s+deleteDocumentsOf(\w+)Behavior" + _PARAMETERS_AND_FUNCTION_BODY_REGEX, re.DOTALL
    )
    _DELETE_DOCUMENTS_BEHAVIOR_METHOD_NAME_REGEX = re.compile(r"deleteDocumentsOf(\w+)Behavior")
    _GENERIC_DELETE_DOCUMENTS_BEHAVIOR_METHOD_REGEX = re.compile(
        r"function\s+" + GENERIC_DELETE_DOCUMENTS_BEHAVIOR_FUNCTION_NAME + _PARAMETERS_AND_FUNCTION_BODY_REGEX,
        re.DOTALL,
    )

    def __init__(
        self,
        name: str = None,
        disabled: bool = False,
        collections: List[str] = None,
        apply_to_all_documents: bool = False,
        script: str = None,
        document_id_postfix: str = None,
    ):
        self.name = name
        self.disabled = disabled
        self.collections: List[str] = collections or []
        self.apply_to_all_documents = apply_to_all_documents
        self.script = script
        self.document_id_postfix = document_id_postfix

        # Internal state set during validation
        self._cached_collections: Optional[List[str]] = None
        self.is_empty_script: bool = False
        self.collection_to_delete_documents_behavior_function: Optional[Dict[str, str]] = None
        self.is_adding_attachments: bool = False
        self.is_loading_attachments: bool = False

        # Nested transformation validators
        self.counters = CountersTransformation(self)
        self.time_series = TimeSeriesTransformation(self)

    def validate(self, errors: List[str], etl_type: "EtlType") -> bool:
        """
        Validates the transformation configuration.

        Args:
            errors: List to append validation errors to.
            etl_type: The type of ETL this transformation is for.

        Returns:
            True if validation passed (no errors), False otherwise.
        """
        from ravendb.documents.operations.etl.etl_type import EtlType

        if errors is None:
            raise ValueError("errors cannot be None")

        if not self.name or not self.name.strip():
            errors.append("Script name cannot be empty")

        if self.apply_to_all_documents:
            if self.collections and len(self.collections) > 0:
                errors.append(
                    f"Collections cannot be specified when ApplyToAllDocuments is set. Script name: '{self.name}'"
                )
        else:
            if not self.collections or len(self.collections) == 0:
                errors.append(
                    f"Collections need be specified or ApplyToAllDocuments has to be set. Script name: '{self.name}'"
                )

        if self.script and self.script.strip():
            # Check for legacy replicateTo method
            if self._LEGACY_REPLICATE_TO_METHOD_REGEX.search(self.script):
                errors.append(
                    f"Found `replicateTo<TableName>()` method in '{self.name}' script which is not supported. "
                    "If you are using the SQL replication script from RavenDB 3.x version then please use `loadTo<TableName>()` instead."
                )

            self.is_adding_attachments = bool(self._ADD_ATTACHMENT_METHOD_REGEX.search(self.script))
            self.is_loading_attachments = bool(self._LOAD_ATTACHMENT_METHOD_REGEX.search(self.script))

            # Validate counters and time series
            self.counters.validate(errors, etl_type)
            self.time_series.validate(errors, etl_type)

            # Validate delete behaviors
            self._validate_delete_behaviors(errors, etl_type)

            # Check for loadTo calls
            collections_from_script = self.get_collections_from_script()
            if not collections_from_script:
                self._check_empty_script(errors, etl_type)
        else:
            self.is_empty_script = True

        if self.is_empty_script:
            if etl_type not in (EtlType.RAVEN, EtlType.EMBEDDINGS_GENERATION, EtlType.GEN_AI):
                errors.append(f"Script '{self.name}' must not be empty")

        return len(errors) == 0

    def _validate_delete_behaviors(self, errors: List[str], etl_type: "EtlType") -> None:
        from ravendb.documents.operations.etl.etl_type import EtlType

        script = self.script or ""
        delete_behaviors = list(self._DELETE_DOCUMENTS_BEHAVIOR_METHOD_REGEX.finditer(script))

        if delete_behaviors:
            if etl_type == EtlType.SQL:
                errors.append("Delete documents behavior functions aren't supported by SQL ETL")
            else:
                self.collection_to_delete_documents_behavior_function = {}

                for match in delete_behaviors:
                    if len(match.groups()) != 1:
                        errors.append(
                            "Invalid delete documents behavior function. It is expected to have the following signature: "
                            "deleteDocumentsOf<CollectionName>Behavior(docId) and return 'true' if document deletion should be sent to a destination"
                        )
                        continue

                    function = match.group(0)
                    collection = match.group(1)

                    function_name_match = self._DELETE_DOCUMENTS_BEHAVIOR_METHOD_NAME_REGEX.search(function)
                    function_name = function_name_match.group(0) if function_name_match else ""

                    if collection not in self.collections:
                        script_collections = ", ".join(f"'{c}'" for c in self.collections)
                        errors.append(
                            f"There is '{function_name}' function defined in '{self.name}' script while the processed collections "
                            f"({script_collections}) doesn't include '{collection}'. "
                            "deleteDocumentsOf<CollectionName>Behavior() function is meant to be defined only for documents from collections that "
                            "are loaded to the same collection on a destination side"
                        )

                    self.collection_to_delete_documents_behavior_function[collection] = function_name

        # Check for generic delete behavior
        generic_delete_behaviors = list(self._GENERIC_DELETE_DOCUMENTS_BEHAVIOR_METHOD_REGEX.finditer(script))

        if generic_delete_behaviors:
            if etl_type == EtlType.SQL:
                errors.append("Delete documents behavior functions aren't supported by SQL ETL")
            else:
                if len(generic_delete_behaviors) > 1:
                    errors.append("Generic delete behavior function can be defined just once in the script")
                else:
                    if self.collection_to_delete_documents_behavior_function is None:
                        self.collection_to_delete_documents_behavior_function = {}
                    self.collection_to_delete_documents_behavior_function[
                        self.GENERIC_DELETE_DOCUMENTS_BEHAVIOR_FUNCTION_KEY
                    ] = self.GENERIC_DELETE_DOCUMENTS_BEHAVIOR_FUNCTION_NAME

    def _check_empty_script(self, errors: List[str], etl_type: "EtlType") -> None:
        from ravendb.documents.operations.etl.etl_type import EtlType

        script = self.script or ""
        actual_script = script

        # Skip all delete behavior functions to check if we have empty transformation
        delete_behaviors = list(self._DELETE_DOCUMENTS_BEHAVIOR_METHOD_REGEX.finditer(script))
        for match in delete_behaviors:
            actual_script = actual_script.replace(match.group(0), "")

        generic_delete_behaviors = list(self._GENERIC_DELETE_DOCUMENTS_BEHAVIOR_METHOD_REGEX.finditer(script))
        if len(generic_delete_behaviors) == 1:
            actual_script = actual_script.replace(generic_delete_behaviors[0].group(0), "")

        if actual_script.strip():
            target_name_map = {
                EtlType.RAVEN: "Collection",
                EtlType.SQL: "Table",
                EtlType.OLAP: "Table",
                EtlType.ELASTIC_SEARCH: "Index",
                EtlType.QUEUE: "Queue",
            }
            target_name = target_name_map.get(etl_type)
            if target_name is None:
                raise ValueError(f"Unknown ETL type: {etl_type}")

            errors.append(f"No `loadTo<{target_name}Name>()` method call found in '{self.name}' script")
        else:
            self.is_empty_script = True

    def get_collections_from_script(self) -> Optional[List[str]]:
        """
        Extracts collection names from loadTo calls in the script.

        Returns:
            List of collection names found in the script, or None if no loadTo calls found.
        """
        if self._cached_collections is not None:
            return self._cached_collections

        script = self.script or ""
        if not script:
            return None

        matches = self._LOAD_TO_METHOD_REGEX.findall(script)
        matches_alt = self._LOAD_TO_METHOD_REGEX_ALT.findall(script)

        if not matches and not matches_alt:
            return None

        collections = list(matches)

        # Handle alternative syntax loadTo('CollectionName') or loadTo("CollectionName")
        for match in matches_alt:
            # match is a tuple of (group1, group2) - one will be empty
            collection = match[0] if match[0] else match[1]
            if collection:
                collections.append(collection)

        self._cached_collections = collections
        return self._cached_collections

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "Disabled": self.disabled,
            "Collections": self.collections,
            "ApplyToAllDocuments": self.apply_to_all_documents,
            "Script": self.script,
            "DocumentIdPostfix": self.document_id_postfix,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "Transformation":
        return cls(
            name=json_dict.get("Name"),
            disabled=json_dict.get("Disabled", False),
            collections=json_dict.get("Collections", []),
            apply_to_all_documents=json_dict.get("ApplyToAllDocuments", False),
            script=json_dict.get("Script"),
            document_id_postfix=json_dict.get("DocumentIdPostfix"),
        )
