from __future__ import annotations
from enum import Enum
from typing import Optional, Type, TypeVar

from ravendb.documents.indexes.definitions import AbstractCommonApiForIndexes


class SearchOperator(Enum):
    OR = "Or"
    AND = "And"


_TIndex = TypeVar("_TIndex", bound=AbstractCommonApiForIndexes)


class Query:
    def __init__(self):
        self.__collection_name: Optional[str] = None
        self.__index_name: Optional[str] = None
        self.__index_type: Optional[Type[_TIndex]] = None

    @property
    def collection(self) -> str:
        return self.__collection_name

    @property
    def index_name(self) -> str:
        return self.__index_name

    @property
    def index_type(self) -> Type[_TIndex]:
        return self.__index_type

    @staticmethod
    def from_collection_name(collection_name: str) -> Query:
        query = Query()
        query.__collection_name = collection_name
        return query

    @staticmethod
    def from_index_name(index_name: str) -> Query:
        query = Query()
        query.__index_name = index_name
        return query

    @staticmethod
    def from_index_type(index_type: Type[_TIndex]) -> Query:
        query = Query()
        query.__index_type = index_type
        return query
