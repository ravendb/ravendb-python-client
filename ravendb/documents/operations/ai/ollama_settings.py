from typing import Dict, Any

from ravendb.documents.operations.ai.abstract_ai_settings import AbstractAiSettings


class OllamaSettings(AbstractAiSettings):
    def __init__(
        self,
        uri: str = None,
        model: str = None,
        think: bool = None,
        temperature: float = None,
        embeddings_max_concurrent_batches: int = None,
    ):
        super().__init__()
        self.uri = uri
        self.model = model
        self.think = think
        self.temperature = temperature
        self.embeddings_max_concurrent_batches = embeddings_max_concurrent_batches

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "OllamaSettings":
        return cls(
            uri=json_dict["Uri"] if "Uri" in json_dict else None,
            model=json_dict["Model"] if "Model" in json_dict else None,
            think=json_dict["Think"] if "Think" in json_dict else None,
            temperature=json_dict["Temperature"] if "Temperature" in json_dict else None,
            embeddings_max_concurrent_batches=(
                json_dict["EmbeddingsMaxConcurrentBatches"] if "EmbeddingsMaxConcurrentBatches" in json_dict else None
            ),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "Uri": self.uri,
            "Model": self.model,
            "Think": self.think,
            "Temperature": self.temperature,
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
        }
