from typing import Generic, TypeVar, Optional

_T = TypeVar("_T")


class Revision(Generic[_T]):
    def __init__(self, previous: Optional[_T] = None, current: Optional[_T] = None):
        self.previous = previous
        self.current = current
