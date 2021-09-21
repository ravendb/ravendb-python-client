import datetime
from typing import Optional

from pyravendb.loaders.include_builder_base import IncludeBaseBuilder
from pyravendb.data import document_conventions


class QueryIncludeBuilder(IncludeBaseBuilder):
    def __init__(self, conventions: document_conventions):
        super(QueryIncludeBuilder, self).__init__(conventions)

    def include_counters(self, *names: str, path: Optional[str] = ""):
        super()._include_counter_with_alias(path, *names)
        return self

    def include_all_counters(self, path: Optional[str] = ""):
        super()._include_all_counters_with_alias(path)
        return self

    def include_documents(self, path: str):
        super()._include_documents(path)
        return self

    def include_time_series(
        self,
        name: str,
        from_date: Optional[datetime.datetime] = None,
        to_date: Optional[datetime.datetime] = None,
        alias: Optional[str] = "",
    ):
        super()._include_time_series(alias, name, from_date, to_date)
        return self

    def include_compare_exchange_value(self, path: str):
        super()._include_compare_exchange_value(path)
        return self
