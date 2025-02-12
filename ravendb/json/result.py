from typing import List, Any, Dict, Optional


class BatchCommandResult:
    def __init__(self, results: Optional[List[Dict]], transaction_index: Optional[int]):
        self.results = results
        self.transaction_index = transaction_index

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "BatchCommandResult":
        return cls(json_dict["Results"], json_dict["TransactionIndex"] if "TransactionIndex" in json_dict else None)


class JsonArrayResult:
    def __init__(self, results: List = None):
        self.results = results

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]):
        return cls(json_dict["Results"])
