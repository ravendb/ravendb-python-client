from __future__ import annotations

import datetime
import json
from typing import Dict, Optional, List, Any, TYPE_CHECKING, Callable, Set
import requests

from ravendb.primitives.constants import int_max
from ravendb.documents.session.loaders.include import TimeSeriesIncludeBuilder
from ravendb.documents.session.time_series import TimeSeriesEntry, AbstractTimeSeriesRange
from ravendb.http.http_cache import HttpCache
from ravendb.http.server_node import ServerNode
from ravendb.http.topology import RaftCommand
from ravendb.http.raven_command import RavenCommand, VoidRavenCommand
from ravendb.documents.operations.definitions import MaintenanceOperation, IOperation, VoidOperation
from ravendb.primitives.time_series import TimeValue
from ravendb.tools.utils import Utils, CaseInsensitiveDict
from ravendb.util.util import RaftIdGenerator
from ravendb.documents.conventions import DocumentConventions

if TYPE_CHECKING:
    from ravendb.documents.store.definition import DocumentStore


class TimeSeriesPolicy:
    def __init__(
        self,
        name: Optional[str] = None,
        aggregation_time: Optional[TimeValue] = None,
        retention_time: TimeValue = TimeValue.MAX_VALUE(),
    ):
        if not name or name.isspace():
            raise ValueError("Name cannot be None or empty")

        if aggregation_time and aggregation_time.compare_to(TimeValue.ZERO()) <= 0:
            raise ValueError("Aggregation time must be greater than zero")

        if retention_time is None or retention_time.compare_to(TimeValue.ZERO()) <= 0:
            raise ValueError("Retention time must be greater than zero")

        self.retention_time = retention_time
        self.aggregation_time = aggregation_time

        self.name = name

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeSeriesPolicy:
        return cls(
            json_dict["Name"],
            TimeValue.from_json(json_dict["AggregationTime"]),
            TimeValue.from_json(json_dict["RetentionTime"]),
        )

    def get_time_series_name(self, raw_name: str) -> str:
        return raw_name + TimeSeriesConfiguration.TIME_SERIES_ROLLUP_SEPARATOR + self.name

    def to_json(self) -> Dict[str, Any]:
        return {
            "Name": self.name,
            "AggregationTime": self.aggregation_time.to_json() if self.aggregation_time else None,
            "RetentionTime": self.retention_time.to_json(),
        }


class RawTimeSeriesPolicy(TimeSeriesPolicy):
    POLICY_STRING = "rawpolicy"  # must be lower case

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> RawTimeSeriesPolicy:
        return cls(TimeValue.from_json(json_dict["RetentionTime"]))

    @classmethod
    def DEFAULT_POLICY(cls) -> RawTimeSeriesPolicy:
        return cls(TimeValue.MAX_VALUE())

    def __init__(self, retention_time: TimeValue = TimeValue.MAX_VALUE()):
        if retention_time.compare_to(TimeValue.ZERO()) <= 0:
            raise ValueError("Retention time must be greater than zero")
        super().__init__(self.POLICY_STRING, retention_time=retention_time)


class TimeSeriesCollectionConfiguration:
    def __init__(
        self,
        disabled: Optional[bool] = False,
        policies: Optional[List[TimeSeriesPolicy]] = None,
        raw_policy: Optional[RawTimeSeriesPolicy] = RawTimeSeriesPolicy.DEFAULT_POLICY(),
    ):
        self.disabled = disabled
        self.policies = policies
        self.raw_policy = raw_policy

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeSeriesCollectionConfiguration:
        return cls(
            json_dict["Disabled"],
            [TimeSeriesPolicy.from_json(policy_json) for policy_json in json_dict["Policies"]],
            RawTimeSeriesPolicy.from_json(json_dict["RawPolicy"]),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Disabled": self.disabled,
            "Policies": [policy.to_json() for policy in self.policies],
            "RawPolicy": self.raw_policy.to_json(),
        }


class TimeSeriesConfiguration:
    TIME_SERIES_ROLLUP_SEPARATOR = "@"

    @classmethod
    def from_json(
        cls,
        json_dict: Dict[str, Any] = None,
    ) -> TimeSeriesConfiguration:
        configuration = cls()
        configuration.collections = {
            key: TimeSeriesCollectionConfiguration.from_json(value) for key, value in json_dict["Collections"].items()
        }
        configuration.policy_check_frequency = (
            Utils.string_to_timedelta(json_dict["PolicyCheckFrequency"])
            if "PolicyCheckFrequency" in json_dict and json_dict["PolicyCheckFrequency"]
            else None
        )
        configuration.named_values = json_dict["NamedValues"]
        configuration._internal_post_json_deserialization()
        return configuration

    def __init__(self):
        self.collections: Dict[str, TimeSeriesCollectionConfiguration] = {}
        self.policy_check_frequency: Optional[datetime.timedelta] = None
        self.named_values: Optional[Dict[str, Dict[str, List[str]]]] = None

    def to_json(self) -> Dict[str, Any]:
        return {
            "Collections": {key: value.to_json() for key, value in self.collections.items()},
            "PolicyCheckFrequency": Utils.timedelta_to_str(self.policy_check_frequency),
            "NamedValues": self.named_values,
        }

    def get_names(self, collection: str, time_series: str) -> Optional[List[str]]:
        if self.named_values is None:
            return None

        ts_holder = self.named_values.get(collection, None)
        if ts_holder is None:
            return None

        names = ts_holder.get(time_series, None)
        if names is None:
            return None

        return names

    def _internal_post_json_deserialization(self) -> None:
        self._populate_named_values()
        self._populate_policies()

    def _populate_policies(self) -> None:
        if self.collections is None:
            return

        dic = CaseInsensitiveDict()
        for key, value in self.collections.items():
            dic[key] = value

        self.collections = dic

    def _populate_named_values(self) -> None:
        if self.named_values is None:
            return

        # ensure ignore case
        dic = CaseInsensitiveDict()

        for key, value in self.named_values.items():
            value_map = CaseInsensitiveDict()
            value_map.update(value)
            dic[key] = value_map

        self.named_values = dic


class ConfigureTimeSeriesOperationResult:
    def __init__(self, raft_command_index: int):
        self.raft_command_index = raft_command_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]):
        return cls(json_dict["RaftCommandIndex"])


class ConfigureTimeSeriesPolicyOperation(MaintenanceOperation[ConfigureTimeSeriesOperationResult]):
    def __init__(self, collection: str, config: TimeSeriesPolicy):
        self.collection = collection
        self.config = config

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[ConfigureTimeSeriesOperationResult]":
        return self.ConfigureTimeSeriesPolicyCommand(self.collection, self.config)

    class ConfigureTimeSeriesPolicyCommand(RavenCommand[ConfigureTimeSeriesOperationResult], RaftCommand):
        def __init__(self, collection: str, configuration: TimeSeriesPolicy):
            if not configuration:
                raise ValueError("Configuration cannot be None")
            if not collection or collection.isspace():
                raise ValueError("Collection cannot be None or empty")

            super().__init__(ConfigureTimeSeriesOperationResult)

            self._collection = collection
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            request = requests.Request(
                "PUT",
                f"{node.url}/databases/{node.database}/admin/timeseries" f"/policy?collection={self._collection}",
                data=self._configuration.to_json(),
            )
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()
            self.result = ConfigureTimeSeriesOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class ConfigureRawTimeSeriesPolicyOperation(ConfigureTimeSeriesPolicyOperation):
    def __init__(self, collection: str, config: RawTimeSeriesPolicy):
        super(ConfigureRawTimeSeriesPolicyOperation, self).__init__(collection, config)


class ConfigureTimeSeriesOperationResult:
    def __init__(self, raft_command_index: int = None):
        self.raft_command_index = raft_command_index

    def to_json(self) -> Dict:
        return {
            "RaftCommandIndex": self.raft_command_index,
        }

    @classmethod
    def from_json(cls, json_dict: Dict) -> ConfigureTimeSeriesOperationResult:
        return cls(json_dict.get("RaftCommandIndex"))


class ConfigureTimeSeriesValueNamesOperation(MaintenanceOperation[ConfigureTimeSeriesOperationResult]):
    class Parameters:
        def __init__(self, collection: str, time_series: str, value_names: List[str], update: Optional[bool] = None):
            self.collection = collection
            self.time_series = time_series
            self.value_names = value_names
            self.update = update

        def validate(self) -> None:
            if not self.collection or self.collection.isspace():
                raise ValueError("Collection name cannot be None or empty")
            if not self.time_series or self.time_series.isspace():
                raise ValueError("TimeSeries cannot be None or empty")
            if not self.value_names:
                raise ValueError("ValueNames cannot be None or empty")

        def to_json(self) -> Dict[str, Any]:
            return {
                "Collection": self.collection,
                "TimeSeries": self.time_series,
                "ValueNames": self.value_names,
                "Update": self.update,
            }

    def __init__(self, parameters: Parameters):
        if parameters is None:
            raise ValueError("Parameters cannot be None")
        super(ConfigureTimeSeriesValueNamesOperation, self).__init__()
        self._parameters = parameters
        self._parameters.validate()

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[ConfigureTimeSeriesOperationResult]":
        return self.ConfigureTimeSeriesValueNamesCommand(self._parameters)

    class ConfigureTimeSeriesValueNamesCommand(RavenCommand[ConfigureTimeSeriesOperationResult], RaftCommand):
        def __init__(self, parameters: ConfigureTimeSeriesValueNamesOperation.Parameters):
            super().__init__(ConfigureTimeSeriesOperationResult)
            self._parameters = parameters

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/timeseries/names/config"
            request = requests.Request("POST", url)
            request.data = self._parameters.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if not response:
                self._throw_invalid_response()

            self.result = ConfigureTimeSeriesOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class RemoveTimeSeriesPolicyOperation(MaintenanceOperation[ConfigureTimeSeriesOperationResult]):
    def __init__(self, collection: str, name: str):
        if not collection or collection.isspace():
            raise ValueError("Collection cannot be None or empty")

        if not name or name.isspace():
            raise ValueError("Name cannot be None or empty")

        self._collection = collection
        self._name = name

    def get_command(self, conventions: "DocumentConventions") -> RavenCommand[ConfigureTimeSeriesOperationResult]:
        return self.RemoveTimeSeriesPolicyCommand(self._collection, self._name)

    class RemoveTimeSeriesPolicyCommand(RavenCommand[ConfigureTimeSeriesOperationResult], RaftCommand):
        def __init__(self, collection: str, name: str):
            super().__init__(ConfigureTimeSeriesOperationResult)
            self._collection = collection
            self._name = name

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "DELETE",
                f"{node.url}/databases"
                f"/{node.database}/admin/timeseries/policy?"
                f"collection={self._collection}&"
                f"name={self._name}",
            )

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                self._throw_invalid_response()

            self.result = ConfigureTimeSeriesOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()


class TimeSeriesOperation:
    class AppendOperation:
        def __init__(self, timestamp: datetime.datetime, values: List[float], tag: Optional[str] = None):
            self.timestamp = timestamp
            self.values = values
            self.tag = tag

        def __eq__(self, other):
            return isinstance(other, TimeSeriesOperation.AppendOperation) and other.timestamp == self.timestamp

        def __hash__(self):
            return hash(self.timestamp)

        def to_json(self) -> Dict[str, Any]:
            json_dict = {
                "Timestamp": Utils.datetime_to_string(self.timestamp),
                "Values": self.values,
            }
            if self.tag:
                json_dict.update({"Tag": self.tag})

            return json_dict

    class DeleteOperation:
        def __init__(
            self, datetime_from: Optional[datetime.datetime] = None, datetime_to: Optional[datetime.datetime] = None
        ):
            self._datetime_from = datetime_from
            self._datetime_to = datetime_to

        def to_json(self) -> Dict[str, Any]:
            return {
                "From": Utils.datetime_to_string(self._datetime_from) if self._datetime_from else None,
                "To": Utils.datetime_to_string(self._datetime_to) if self._datetime_to else None,
            }

    def __init__(self, name: Optional[str] = None):
        self.name = name
        self._appends: Set[TimeSeriesOperation.AppendOperation] = set()
        self._deletes: List[TimeSeriesOperation.DeleteOperation] = []

    def to_json(self) -> Dict[str, Any]:
        json_dict = {"Name": self.name}
        if self._appends:
            json_dict["Appends"] = [append_op.to_json() for append_op in self._appends]
        if self._deletes:
            json_dict["Deletes"] = [delete_op.to_json() for delete_op in self._deletes]
        return json_dict

    def append(self, append_operation: AppendOperation) -> None:
        if self._appends is None:
            self._appends = set()

        if append_operation in self._appends:
            # __eq__ override lets us discard old one by passing new one due to the same timestamps
            self._appends.discard(append_operation)

        self._appends.add(append_operation)

    def delete(self, delete_operation: DeleteOperation) -> None:
        if self._deletes is None:
            self._deletes = []

        self._deletes.append(delete_operation)


class TimeSeriesRangeResult:
    def __init__(
        self,
        from_date: datetime,
        to_date: datetime,
        entries: List[TimeSeriesEntry],
        total_results: int = None,
        includes: Optional[Dict[str, Any]] = None,
    ):
        self.from_date = from_date
        self.to_date = to_date
        self.entries = entries if entries else []
        self.total_results = total_results
        self.includes = includes

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeSeriesRangeResult:
        return cls(
            Utils.string_to_datetime(json_dict["From"]),
            Utils.string_to_datetime(json_dict["To"]),
            [TimeSeriesEntry.from_json(entry_json) for entry_json in json_dict["Entries"]],
            json_dict["TotalResults"] if "TotalResults" in json_dict else None,
            json_dict.get("Includes", None),
        )


class GetTimeSeriesOperation(IOperation[TimeSeriesRangeResult]):
    def __init__(
        self,
        doc_id: str,
        time_series: str,
        from_date: datetime.datetime = None,
        to_date: datetime.datetime = None,
        start: int = 0,
        page_size: int = int_max,
        includes: Optional[Callable[[TimeSeriesIncludeBuilder], None]] = None,
    ):
        if not doc_id or doc_id.isspace():
            raise ValueError("DocId cannot be None or empty")
        if not time_series or time_series.isspace():
            raise ValueError("Timeseries cannot be None or empty")

        self._doc_id = doc_id
        self._start = start
        self._page_size = page_size
        self._name = time_series
        self._from = from_date
        self._to = to_date
        self._includes = includes

    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: HttpCache
    ) -> "RavenCommand[TimeSeriesRangeResult]":
        return self.GetTimeSeriesCommand(
            self._doc_id, self._name, self._from, self._to, self._start, self._page_size, self._includes
        )

    class GetTimeSeriesCommand(RavenCommand[TimeSeriesRangeResult]):
        def __init__(
            self,
            doc_id: str,
            name: str,
            from_date: datetime.datetime,
            to_date: datetime.datetime,
            start: int,
            page_size: int,
            includes: Callable[[TimeSeriesIncludeBuilder], None],
        ):
            super().__init__(TimeSeriesRangeResult)
            self._doc_id = doc_id
            self._name = name
            self._start = start
            self._page_size = page_size
            self._from = from_date
            self._to = to_date
            self._includes = includes

        def create_request(self, node: ServerNode) -> requests.Request:
            path_builder = [
                node.url,
                "/databases/",
                node.database,
                "/timeseries",
                "?docId=",
                self._doc_id,
            ]

            if self._start > 0:
                path_builder.append("&start=")
                path_builder.append(str(self._start))

            if self._page_size < int_max:
                path_builder.append("&pageSize=")
                path_builder.append(str(self._page_size))

            path_builder.append("&name=")
            path_builder.append(self._name)

            if self._from is not None:
                path_builder.append("&from=")
                path_builder.append(Utils.datetime_to_string(self._from))

            if self._to is not None:
                path_builder.append("&to=")
                path_builder.append(Utils.datetime_to_string(self._to))

            if self._includes is not None:
                self.add_includes_to_request(path_builder, self._includes)

            return requests.Request("GET", "".join(path_builder))

        @staticmethod
        def add_includes_to_request(
            path_builder: List[str], includes: Callable[[TimeSeriesIncludeBuilder], None]
        ) -> None:
            include_builder = TimeSeriesIncludeBuilder(DocumentConventions.default_conventions())
            includes(include_builder)

            if include_builder._include_time_series_document:
                path_builder.append("&includeDocument=true")

            if include_builder._include_time_series_tags:
                path_builder.append("&includeTags=true")

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                return

            self.result = TimeSeriesRangeResult.from_json(json.loads(response))

        def is_read_request(self) -> bool:
            return True


class TimeSeriesDetails:
    def __init__(self, key: str, values: Dict[str, List[TimeSeriesRangeResult]]):
        self.key = key
        self.values = values

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeSeriesDetails:
        return cls(
            json_dict["Id"],
            {
                key: [TimeSeriesRangeResult.from_json(value) for value in values]
                for key, values in json_dict["Values"].items()
            },
        )


class GetMultipleTimeSeriesOperation(IOperation[TimeSeriesDetails]):
    def __init__(
        self,
        doc_id: str,
        ranges: List[AbstractTimeSeriesRange],
        start: Optional[int] = 0,
        page_size: Optional[int] = int_max,
        includes: Optional[Callable[[TimeSeriesIncludeBuilder], None]] = None,
    ):
        if not doc_id or doc_id.isspace():
            raise ValueError("DocId cannot be None or empty")
        if ranges is None:
            raise ValueError("Ranges cannot be None")
        self._doc_id = doc_id
        self._ranges = ranges
        self._start = start
        self._page_size = page_size
        self._includes = includes

    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: HttpCache
    ) -> "GetMultipleTimeSeriesOperation.GetMultipleTimeSeriesCommand":
        return self.GetMultipleTimeSeriesCommand(
            self._doc_id, self._ranges, self._start, self._page_size, self._includes
        )

    class GetMultipleTimeSeriesCommand(RavenCommand[TimeSeriesDetails]):
        def __init__(
            self,
            doc_id: str,
            ranges: List[AbstractTimeSeriesRange],
            start: Optional[int] = 0,
            page_size: Optional[int] = int_max,
            includes: Optional[Callable[[TimeSeriesIncludeBuilder], None]] = None,
        ):
            super().__init__(TimeSeriesDetails)

            if doc_id is None:
                raise ValueError("DocId cannot be None")

            self._doc_id = doc_id
            self._ranges = ranges
            self._start = start
            self._page_size = page_size
            self._includes = includes

        def create_request(self, node: ServerNode) -> requests.Request:
            path_builder = [
                node.url,
                "/databases/",
                node.database,
                "/timeseries/ranges",
                "?docId=",
                self._doc_id,
            ]

            if self._start > 0:
                path_builder.append("&start=")
                path_builder.append(str(self._start))

            if self._page_size < int_max:
                path_builder.append("&pageSize=")
                path_builder.append(str(self._page_size))

            if not self._ranges:
                raise ValueError("Ranges cannot be None or empty")

            for range_ in self._ranges:
                if not range_.name or range_.name.isspace():
                    raise ValueError("Missing name argument in TimeSeriesRange. Name cannot be None or empty")

                path_builder.append("&name=")
                path_builder.append(range_.name or "")
                path_builder.append("&from=")
                path_builder.append(Utils.datetime_to_string(range_.from_date) if range_.from_date is not None else "")
                path_builder.append("&to=")
                path_builder.append(Utils.datetime_to_string(range_.to_date) if range_.to_date is not None else "")

            if self._includes is not None:
                GetTimeSeriesOperation.GetTimeSeriesCommand.add_includes_to_request(path_builder, self._includes)

            url = "".join(path_builder)

            return requests.Request("GET", url)

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if response is None:
                return

            self.result = TimeSeriesDetails.from_json(json.loads(response))

        def is_read_request(self) -> bool:
            return True


class TimeSeriesBatchOperation(VoidOperation):
    def __init__(self, document_id: str, operation: TimeSeriesOperation):
        if document_id is None:
            raise ValueError("Document id cannot be None")
        if operation is None:
            raise ValueError("Operation cannot be None")

        self._document_id = document_id
        self._operation = operation

    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: HttpCache
    ) -> VoidRavenCommand:
        return self.TimeSeriesBatchCommand(self._document_id, self._operation, conventions)

    class TimeSeriesBatchCommand(VoidRavenCommand):
        def __init__(self, document_id: str, operation: TimeSeriesOperation, conventions: "DocumentConventions"):
            super().__init__()
            self._document_id = document_id
            self._operation = operation
            self._conventions = conventions

        def create_request(self, node: ServerNode) -> requests.Request:
            url = f"{node.url}/databases/{node.database}/timeseries?docId={self._document_id}"
            request = requests.Request("POST", url)
            request.data = self._operation.to_json()
            return request

        def is_read_request(self) -> bool:
            return False


class TimeSeriesItemDetail:
    def __init__(self, name: str, number_of_entities: int, start_date: datetime.datetime, end_date: datetime.datetime):
        self.name = name
        self.number_of_entries = number_of_entities
        self.start_date = start_date
        self.end_date = end_date

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeSeriesItemDetail:
        return cls(
            json_dict["Name"],
            json_dict["NumberOfEntries"],
            Utils.string_to_datetime(json_dict["StartDate"]),
            Utils.string_to_datetime(json_dict["EndDate"]),
        )


class TimeSeriesStatistics:
    def __init__(self, document_id: str, time_series: List[TimeSeriesItemDetail]):
        self.document_id = document_id
        self.time_series = time_series

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> TimeSeriesStatistics:
        return cls(
            json_dict["DocumentId"],
            [TimeSeriesItemDetail.from_json(ts_item_detail_json) for ts_item_detail_json in json_dict["TimeSeries"]],
        )


class GetTimeSeriesStatisticsOperation(IOperation[TimeSeriesStatistics]):
    def __init__(self, document_id: str):
        self._document_id = document_id

    @property
    def document_id(self) -> str:
        return self.document_id

    def get_command(
        self, store: "DocumentStore", conventions: "DocumentConventions", cache: HttpCache
    ) -> RavenCommand[TimeSeriesStatistics]:
        return self.GetTimeSeriesStatisticsCommand(self._document_id)

    class GetTimeSeriesStatisticsCommand(RavenCommand[TimeSeriesStatistics]):
        def __init__(self, document_id: str):
            super().__init__()
            self._document_id = document_id

        def is_read_request(self) -> bool:
            return True

        def create_request(self, node: ServerNode) -> requests.Request:
            return requests.Request(
                "GET", f"{node.url}/databases/{node.database}/timeseries/stats?docId={self._document_id}"
            )

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            self.result = TimeSeriesStatistics.from_json(json.loads(response))


class ConfigureTimeSeriesOperation(MaintenanceOperation[ConfigureTimeSeriesOperationResult]):
    def __init__(self, configuration: TimeSeriesConfiguration):
        if not configuration:
            raise ValueError("Configuration cannot be None")

        self._configuration = configuration

    def get_command(self, conventions: "DocumentConventions") -> "RavenCommand[ConfigureTimeSeriesOperationResult]":
        return self.ConfigureTimeSeriesCommand(self._configuration)

    class ConfigureTimeSeriesCommand(RavenCommand[ConfigureTimeSeriesOperationResult], RaftCommand):
        def __init__(self, configuration: TimeSeriesConfiguration):
            super().__init__(ConfigureTimeSeriesOperationResult)
            self._configuration = configuration

        def is_read_request(self) -> bool:
            return False

        def create_request(self, node: ServerNode) -> requests.Request:
            request = requests.Request("POST", f"{node.url}/databases/{node.database}/admin/timeseries/config")
            request.data = self._configuration.to_json()
            return request

        def set_response(self, response: Optional[str], from_cache: bool) -> None:
            if not response:
                self._throw_invalid_response()

            self.result = ConfigureTimeSeriesOperationResult.from_json(json.loads(response))

        def get_raft_unique_request_id(self) -> str:
            return RaftIdGenerator.new_id()
