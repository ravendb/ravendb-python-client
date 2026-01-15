from abc import ABC, abstractmethod
from typing import Dict, Any


class AbstractAiSettings(ABC):
    def __init__(self, embeddings_max_concurrent_batches: int = None):
        self.embeddings_max_concurrent_batches = embeddings_max_concurrent_batches

    @classmethod
    @abstractmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> Any:
        pass

    @abstractmethod
    def to_json(self) -> Dict[str, Any]:
        pass
