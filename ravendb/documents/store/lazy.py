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
