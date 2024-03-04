from __future__ import annotations
from abc import abstractmethod
from typing import TypeVar, TYPE_CHECKING, Dict, List, Type, Optional, Set, Union

from ravendb.documents.store.lazy import Lazy

if TYPE_CHECKING:
    from ravendb.documents.session.document_session import DocumentSession

_T = TypeVar("_T")


class LoaderWithInclude:
    @abstractmethod
    def include(self, path: str) -> LoaderWithInclude:
        pass

    @abstractmethod
    def load(self, id_: str, object_type: Optional[Type[_T]] = None) -> _T:
        pass


class MultiLoaderWithInclude(LoaderWithInclude):
    def __init__(self, session: DocumentSession):
        self.__session = session
        self.__includes: Set[str] = set()

    def include(self, path: str) -> LoaderWithInclude:
        self.__includes.add(path)
        return self

    def load(self, id_or_ids: Union[List[str], str], object_type: Optional[Type[_T]] = None) -> _T:
        if not isinstance(id_or_ids, (str, list)):
            raise TypeError(f"Expected str or list of str, got '{type(id_or_ids)}'")
        ids = [id_or_ids] if isinstance(id_or_ids, str) else id_or_ids
        return self.__session._load_internal(object_type, ids, self.__includes)


class LazyMultiLoaderWithInclude(LoaderWithInclude):
    def __init__(self, session: "DocumentSession"):
        self.__session = session
        self.__includes = []

    def include(self, path: str) -> LazyMultiLoaderWithInclude:
        self.__includes.append(path)
        return self

    def load(self, id_or_ids: Union[List[str], str], object_type: Optional[Type[_T]] = None) -> Lazy[Dict[str, _T]]:
        if not isinstance(id_or_ids, (str, list)):
            raise TypeError(f"Expected str or list of str, got '{type(id_or_ids)}'")
        ids = [id_or_ids] if isinstance(id_or_ids, str) else id_or_ids
        return self.__session.lazy_load_internal(object_type, ids, self.__includes, None)
