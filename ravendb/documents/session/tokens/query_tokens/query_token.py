from abc import abstractmethod
from typing import List


class QueryToken:
    RQL_KEYWORDS = {"as", "select", "where", "load", "group", "order", "include"}

    @abstractmethod
    def write_to(self, writer: List[str]) -> None:
        raise NotImplementedError("Error : write_to is not implemented")

    def write_field(self, writer: List[str], field: str):
        key_word = self.is_keyword(field)
        if key_word:
            writer.append("'")
        writer.append(field)
        if key_word:
            writer.append("'")

    @staticmethod
    def is_keyword(field: str) -> bool:
        return field in QueryToken.RQL_KEYWORDS
