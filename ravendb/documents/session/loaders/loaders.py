from __future__ import annotations
from abc import abstractmethod
from typing import TypeVar, TYPE_CHECKING, Dict, List, Type

from ravendb.documents.store.lazy import Lazy

if TYPE_CHECKING:
    from ravendb.documents.session.document_session import DocumentSession

_T = TypeVar("_T")


class LoaderWithInclude:
    @abstractmethod
    def include(self, path: str) -> LoaderWithInclude:
        pass

    @abstractmethod
    def load(self, object_type: type, *ids: str) -> Dict[str, object]:
        pass


class MultiLoaderWithInclude(LoaderWithInclude):
    def __init__(self, session: DocumentSession):
        self.__session = session
        self.__includes: List[str] = []

    def include(self, path: str) -> LoaderWithInclude:
        self.__includes.append(path)
        return self

    def load(self, object_type: Type[_T], *ids: str) -> Dict[str, object]:
        return self.__session._load_internal(object_type, list(ids), self.__includes)


class LazyMultiLoaderWithInclude(LoaderWithInclude):
    def __init__(self, session: "DocumentSession"):
        self.__session = session
        self.__includes = []

    def include(self, path: str) -> LazyMultiLoaderWithInclude:
        self.__includes.append(path)
        return self

    def load(self, object_type: Type[_T], *ids: str) -> Lazy[Dict[str, object]]:
        return self.__session.lazy_load_internal(object_type, list(ids), self.__includes, None)
