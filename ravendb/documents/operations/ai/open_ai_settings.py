from typing import Dict, Any

from ravendb.documents.operations.ai.open_ai_chat_api_settings import OpenAiChatApiSettings, OpenAiChatApiType


class OpenAiSettings(OpenAiChatApiSettings):
    def __init__(
        self,
        api_key: str = None,
        endpoint: str = None,
        model: str = None,
        organization_id: str = None,
        project_id: str = None,
        dimensions: int = None,
        temperature: float = None,
        embeddings_max_concurrent_batches: int = None,
        reasoning_effort: str = None,
        enable_prompt_cache: bool = None,
        chat_api: OpenAiChatApiType = None,
    ):
        super().__init__(
            api_key,
            endpoint,
            model,
            dimensions,
            temperature,
            embeddings_max_concurrent_batches,
            enable_prompt_cache,
            reasoning_effort,
            chat_api,
        )
        self.organization_id = organization_id
        self.project_id = project_id

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "OpenAiSettings":
        return cls(
            api_key=json_dict.get("ApiKey"),
            endpoint=json_dict.get("Endpoint"),
            model=json_dict.get("Model"),
            dimensions=json_dict["Dimensions"] if "Dimensions" in json_dict else None,
            temperature=json_dict["Temperature"] if "Temperature" in json_dict else None,
            organization_id=json_dict["OrganizationId"] if "OrganizationId" in json_dict else None,
            project_id=json_dict["ProjectId"] if "ProjectId" in json_dict else None,
            embeddings_max_concurrent_batches=(
                json_dict["EmbeddingsMaxConcurrentBatches"] if "EmbeddingsMaxConcurrentBatches" in json_dict else None
            ),
            reasoning_effort=cls._reasoning_effort_from_json(json_dict),
            enable_prompt_cache=json_dict.get("EnablePromptCache"),
            chat_api=cls._chat_api_from_json(json_dict),
        )

    def to_json(self) -> Dict[str, Any]:
        json_dict = {
            "ApiKey": self.api_key,
            "Endpoint": self.endpoint,
            "Model": self.model,
            "Dimensions": self.dimensions,
            "Temperature": self.temperature,
            "OrganizationId": self.organization_id,
            "ProjectId": self.project_id,
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
        }
        if self.enable_prompt_cache is not None:
            json_dict["EnablePromptCache"] = self.enable_prompt_cache
        return self._add_chat_api_fields(json_dict)
