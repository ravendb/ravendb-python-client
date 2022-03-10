from typing import List, Dict, Set

from ravendb.documents.queries.query import QueryResult


class Highlightings:
    def __init__(self, field_name: str):
        self.__field_name = field_name
        self.__highlightings: Dict[str, List[str]] = {}

    @property
    def field_name(self) -> str:
        return self.__field_name

    @property
    def result_indents(self) -> Set[str]:
        return set(self.__highlightings.keys())

    def get_fragments(self, key: str) -> List[str]:
        return self.__highlightings.get(key, None) or []

    def update(self, highlightings: Dict[str, Dict[str, List[str]]]) -> None:
        self.__highlightings.clear()

        if highlightings is None or self.field_name not in highlightings:
            return

        result = highlightings.get(self.field_name)
        for key, value in result.items():
            self.__highlightings[key] = value


class QueryHighlightings:
    def __init__(self):
        self.__highlightings: List[Highlightings] = []

    def add(self, field_name: str) -> Highlightings:
        field_highlightings = Highlightings(field_name)
        self.__highlightings.append(field_highlightings)
        return field_highlightings

    def update(self, query_result: QueryResult) -> None:
        for field_highlightings in self.__highlightings:
            field_highlightings.update(query_result.highlightings)


class HighlightingOptions:
    def __init__(self, group_key: str = None, pre_tags: List[str] = None, post_tags: List[str] = None):
        self.group_key = group_key
        self.pre_tags = pre_tags
        self.post_tags = post_tags

    def to_json(self) -> dict:
        return {"GroupKey": self.group_key, "PreTags": self.pre_tags, "PostTags": self.post_tags}
