import datetime
from typing import Optional

from pyravendb.data import document_conventions
from pyravendb.loaders.include_builder_base import IncludeBaseBuilder


class IncludeBuilder(IncludeBaseBuilder):
    def __init__(self, conventions: document_conventions):
        super(IncludeBuilder, self).__init__(conventions)

    def include_documents(self, path: str) -> IncludeBaseBuilder:
        super(IncludeBuilder, self)._include_documents(path)
        return self

    def include_counter(self, *names: str) -> IncludeBaseBuilder:
        super(IncludeBuilder, self)._include_counters("", *names)
        return self

    def include_all_counters(self) -> IncludeBaseBuilder:
        super(IncludeBuilder, self)._include_all_counters("")
        return self

    def include_time_series(
        self,
        name: str,
        from_date: Optional[datetime.datetime] = None,
        to_date: Optional[datetime.datetime] = None,
        alias: Optional[str] = "",
    ) -> IncludeBaseBuilder:
        super(IncludeBuilder, self)._include_time_series(alias, name, from_date, to_date)
        return self

    def include_compare_exchange_value(self, path: str) -> IncludeBaseBuilder:
        super(IncludeBuilder, self)._include_compare_exchange_value(path)
        return self
