from typing import Dict, Any

from ravendb.documents.operations.ai.abstract_ai_settings import AbstractAiSettings


class OllamaSettings(AbstractAiSettings):
    def __init__(self, uri: str = None, model: str = None):
        super().__init__()
        self.uri = uri
        self.model = model
        self.think: bool = None
        self.temperature: float = None

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "OllamaSettings":
        return cls(
            uri=json_dict["Uri"],
            model=json_dict["Model"],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Uri": self.uri,
            "Model": self.model,
            "Think": self.think,
            "Temperature": self.temperature,
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
        }