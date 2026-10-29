from abc import ABC

from ravendb.documents.operations.ai.abstract_ai_settings import AbstractAiSettings


class OpenAiBaseSettings(AbstractAiSettings, ABC):
    def __init__(
        self,
        api_key: str = None,
        endpoint: str = None,
        model: str = None,
        dimensions: int = None,
        temperature: float = None,
        embeddings_max_concurrent_batches: int = None,
        enable_prompt_cache: bool = None,
    ):
        super().__init__(embeddings_max_concurrent_batches)
        self.api_key = api_key
        self.endpoint = endpoint
        self.model = model
        self.dimensions = dimensions
        self.temperature = temperature
        # Sends a prompt_cache_key in chat completion requests, so providers that support it can reuse prompt prefixes.
        # When not set, the key is sent for AI agents and for GenAI tasks with query tools.
        self.enable_prompt_cache = enable_prompt_cache
