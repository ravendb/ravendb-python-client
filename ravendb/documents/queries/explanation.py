from typing import Dict, List

from ravendb.documents.queries.query import QueryResult


class Explanations:
    def __init__(self):
        self.explanations: Dict[str, List[str]] = {}

    def update(self, query_result: QueryResult) -> None:
        self.explanations = query_result.explanations


class ExplanationOptions:
    def __init__(self, group_key: str = None):
        self.group_key = group_key
