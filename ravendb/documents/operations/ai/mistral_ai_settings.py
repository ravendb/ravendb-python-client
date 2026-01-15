from typing import Dict, Any

from ravendb.documents.operations.ai.abstract_ai_settings import AbstractAiSettings


class MistralAiSettings(AbstractAiSettings):
    def __init__(
        self,
        api_key: str = None,
        model: str = None,
        endpoint: str = None,
        embeddings_max_concurrent_batches: int = None,
    ):
        super().__init__(embeddings_max_concurrent_batches)
        self.api_key = api_key
        self.model = model
        self.endpoint = endpoint

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "MistralAiSettings":
        return cls(
            api_key=json_dict["ApiKey"] if "ApiKey" in json_dict else None,
            model=json_dict["Model"] if "Model" in json_dict else None,
            endpoint=json_dict["Endpoint"] if "Endpoint" in json_dict else None,
            embeddings_max_concurrent_batches=(
                json_dict["EmbeddingsMaxConcurrentBatches"] if "EmbeddingsMaxConcurrentBatches" in json_dict else None
            ),
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "ApiKey": self.api_key,
            "Model": self.model,
            "Endpoint": self.endpoint,
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
        }
