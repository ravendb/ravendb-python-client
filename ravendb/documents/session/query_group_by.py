from __future__ import annotations
from typing import Generic, TypeVar, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ravendb.documents.session.query import DocumentQuery

_T = TypeVar("_T")


class GroupByField:
    def __init__(self, field_name: Optional[str] = None, projected_name: Optional[str] = None):
        self.field_name = field_name
        self.projected_name = projected_name


class GroupByDocumentQuery(Generic[_T]):
    def __init__(self, query: DocumentQuery[_T]):
        self.__query = query

    def select_key(
        self, field_name: Optional[str] = None, projected_name: Optional[str] = None
    ) -> GroupByDocumentQuery[_T]:
        self.__query._group_by_key(field_name, projected_name)
        return self

    def select_sum(self, field: GroupByField, *fields: GroupByField) -> DocumentQuery[_T]:
        if field is None:
            raise ValueError("field cannot be None")

        self.__query._group_by_sum(field.field_name, field.projected_name)

        if not fields:
            return self.__query

        for f in fields:
            self.__query._group_by_sum(f.field_name, f.projected_name)

        return self.__query

    def select_count(self, projected_name: str = "count") -> DocumentQuery[_T]:
        self.__query._group_by_count(projected_name)
        return self.__query
