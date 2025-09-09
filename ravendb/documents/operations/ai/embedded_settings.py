from typing import Dict, Any

from ravendb.documents.operations.ai.abstract_ai_settings import AbstractAiSettings


class EmbeddedSettings(AbstractAiSettings):
    def __init__(self):
        super().__init__()

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "EmbeddedSettings":
        return cls()

    def to_json(self) -> Dict[str, Any]:
        return {"EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches}
