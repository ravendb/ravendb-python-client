from typing import Dict, Any

from ravendb.documents.operations.ai.abstract_ai_settings import AbstractAiSettings


class EmbeddedSettings(AbstractAiSettings):
    def __init__(self, embeddings_max_concurrent_batches: int = None):
        super().__init__(embeddings_max_concurrent_batches)

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "EmbeddedSettings":
        return cls(
            embeddings_max_concurrent_batches=json_dict.get("EmbeddingsMaxConcurrentBatches") if json_dict.get("EmbeddingsMaxConcurrentBatches") else None,
        )

    def to_json(self) -> Dict[str, Any]:
        return {"EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches}
