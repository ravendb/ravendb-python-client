from abc import abstractmethod
from typing import List, Union
from pyravendb.tools.utils import Utils
from pyravendb.data.timeseries import TimeSeriesRange


class QueryToken:
    RQL_KEYWORDS = {"as", "select", "where", "load", "group", "order", "include"}

    @abstractmethod
    def write_to(self, writer: List[str]):
        raise NotImplementedError("Error : write_to is not implemented")

    def write_field(self, writer: List[str], field: str):
        key_word = field in self.RQL_KEYWORDS
        if key_word:
            writer.append("'")
        writer.append(field)
        if key_word:
            writer.append("'")


class CompareExchangeValueIncludesToken(QueryToken):
    def __init__(self, path: str):
        if not path:
            raise ValueError("Path cannot be None")
        self.__path = path

    @staticmethod
    def create(path: str):
        return CompareExchangeValueIncludesToken(path)

    def write_to(self, writer: List[str]):
        writer.append(f"cmpxchg('{self.__path}')")


class CounterIncludesToken(QueryToken):
    def __init__(self, source_path: str, counter_name: Union[None, str], is_all: bool):
        self.__counter_name = counter_name
        self.__all = is_all
        self.__source_path = source_path

    @staticmethod
    def create(source_path: str, counter_name: str):
        return CounterIncludesToken(source_path, counter_name, False)

    @staticmethod
    def all(source_path: str):
        return CounterIncludesToken(source_path, None, True)

    def add_alias_to_path(self, alias: str):
        self.__source_path = alias if not self.__source_path else f"{alias}.{self.__source_path}"

    def write_to(self, writer: List[str]):
        writer.append("counters(")
        if self.__source_path:
            writer.append(self.__source_path)

            if not self.__all:
                writer.append(", ")

        if not self.__all:
            writer.append(f"'{self.__counter_name}'")

        writer.append(")")


class TimeSeriesIncludesToken(QueryToken):
    def __init__(self, source_path: str, time_range: TimeSeriesRange):
        self.__range = time_range
        self.__source_path = source_path

    @staticmethod
    def create(source_path: str, time_range: TimeSeriesRange):
        return TimeSeriesIncludesToken(source_path, time_range)

    def add_alias_to_path(self, alias: str):
        self.__source_path = alias if not self.__source_path else f"{alias}.{self.__source_path}"

    def write_to(self, writer: List[str]):
        writer.append("timeseries(")
        if self.__source_path:
            writer.append(f"{self.__source_path}, ")

        writer.append(f"'{self.__range.name}', ")
        writer.append(f"'{Utils.datetime_to_string(self.__range.from_date)}', " if self.__range.from_date else "null,")
        writer.append(f"'{Utils.datetime_to_string(self.__range.to_date)}', " if self.__range.to_date else "null")
        writer.append(")")
