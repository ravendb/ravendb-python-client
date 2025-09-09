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
    ):
        super().__init__()
        self.api_key = api_key
        self.endpoint = endpoint
        self.model = model
        self.dimensions = dimensions
        self.temperature = temperature
