from __future__ import annotations
import threading
from typing import Generic, Callable, Any, Union, TypeVar

T = TypeVar("T")


class Lazy(Generic[T]):
    def __init__(self, value_factory: Callable[[], Any]):
        self.__value_created = False  # todo: check if it isn't cached - volatile in java
        self.__value_factory = value_factory
        self.__value: Union[None, T] = None
        self.__value_lock = threading.Lock()

    @property
    def is_value_created(self) -> bool:
        return self.__value_created

    @property
    def value(self) -> T:  # double check locking
        if self.__value_created:
            return self.__value
        with self.__value_lock:
            if not self.__value_created:
                self.__value = self.__value_factory()
                self.__value_created = True
        return self.__value


class ConditionalLoadResult(Generic[T]):
    def __init__(self, entity: T = None, change_vector: str = None):
        self.__entity = entity
        self.__change_vector = change_vector

    @property
    def entity(self) -> T:
        return self.__entity

    @property
    def change_vector(self) -> str:
        return self.__change_vector

    @staticmethod
    def create(entity: T, change_vector: str) -> ConditionalLoadResult:
        return ConditionalLoadResult(entity, change_vector)
