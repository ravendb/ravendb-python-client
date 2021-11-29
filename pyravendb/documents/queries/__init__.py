import hashlib
from typing import Iterable


class HashCalculator:
    def __init__(self):
        self.__buffer = []

    def write(self, obj: object):
        if object is None:
            self.__buffer.append("None")
        elif "__str__" in obj.__dict__:
            self.__buffer.append(str(object))
        else:
            raise TypeError("Cannot parse to string, __str__ isn't defined for that object")

    def flush_md5(self) -> str:
        return self.calculate_hash_from_str_collection(self.__buffer)

    @staticmethod
    def calculate_hash_from_str_collection(unique_ids: Iterable[str]) -> str:
        return hashlib.md5(bytes(",".join(unique_ids).encode("utf-8"))).hexdigest()
