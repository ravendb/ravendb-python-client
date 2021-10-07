from __future__ import annotations
from abc import abstractmethod
from typing import TypeVar

from pyravendb.documents.session.document_session import DocumentSession

_T = TypeVar("_T")


class LoaderWithInclude:
    @abstractmethod
    def include(self, path: str) -> LoaderWithInclude:
        pass

    @abstractmethod
    def load(self, object_type: type, *ids: str) -> dict[str, object]:
        pass


class MultiLoaderWithInclude(LoaderWithInclude):
    def __init__(self, session: DocumentSession):
        self.__session = session
        self.__includes: list[str] = []

    def include(self, path: str) -> LoaderWithInclude:
        self.__includes.append(path)
        return self

    def load(self, object_type: type, *ids: str) -> dict[str, object]:
        return self.__session._load_internal(object_type, ids)
