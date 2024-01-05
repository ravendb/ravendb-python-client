from typing import List, Any, Dict


class BatchCommandResult:
    def __init__(self, results, transaction_index):
        self.results: [None, list] = results
        self.transaction_index: [None, int] = transaction_index


class JsonArrayResult:
    def __init__(self, results: List = None):
        self.results = results

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]):
        return cls(json_dict["Results"])
