from typing import Dict, Any, Optional

from ravendb.documents.operations.ai.abstract_ai_settings import AbstractAiSettings


class AnthropicSettings(AbstractAiSettings):
    """Settings for Anthropic's native Claude API (the Messages API). Chat model type only."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        endpoint: Optional[str] = None,
        max_output_tokens: Optional[int] = None,
        reasoning_effort: Optional[str] = None,
        enable_prompt_cache: Optional[bool] = None,
        api_version: Optional[str] = None,
    ):
        super().__init__()
        # Sent as the x-api-key header
        self.api_key = api_key
        # The Claude model id
        self.model = model
        # Optional, the server defaults to "https://api.anthropic.com/v1/"
        self.endpoint = endpoint
        # The output-token cap per request, required by the Messages API. When not set the server applies 16000.
        self.max_output_tokens = max_output_tokens
        # How much reasoning the model does before answering: "low", "medium", "high", "xhigh" or "max".
        # Not every model supports every level, and a model without effort support rejects the request.
        # Sent trimmed and in lowercase without further validation. When not set the model applies its own default.
        self.reasoning_effort = reasoning_effort
        # Enables prompt caching for AI agents and GenAI tasks. When not set, caching is enabled for AI agents and for
        # GenAI tasks with query tools, and disabled for other GenAI tasks.
        self.enable_prompt_cache = enable_prompt_cache
        # Sent as the anthropic-version header. Optional, the server defaults to "2023-06-01".
        self.api_version = api_version

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "AnthropicSettings":
        settings = cls(
            api_key=json_dict.get("ApiKey"),
            model=json_dict.get("Model"),
            endpoint=json_dict.get("Endpoint"),
            max_output_tokens=json_dict.get("MaxOutputTokens"),
            reasoning_effort=json_dict.get("ReasoningEffort"),
            enable_prompt_cache=json_dict.get("EnablePromptCache"),
            api_version=json_dict.get("ApiVersion"),
        )
        settings.embeddings_max_concurrent_batches = json_dict.get("EmbeddingsMaxConcurrentBatches")
        return settings

    def to_json(self) -> Dict[str, Any]:
        json_dict = {
            "EmbeddingsMaxConcurrentBatches": self.embeddings_max_concurrent_batches,
            "Model": self.model,
            "ApiKey": self.api_key,
        }
        if self.endpoint is not None and self.endpoint.strip():
            json_dict["Endpoint"] = self.endpoint
        if self.max_output_tokens is not None:
            json_dict["MaxOutputTokens"] = self.max_output_tokens
        if self.reasoning_effort is not None and self.reasoning_effort.strip():
            json_dict["ReasoningEffort"] = self.reasoning_effort
        if self.enable_prompt_cache is not None:
            json_dict["EnablePromptCache"] = self.enable_prompt_cache
        if self.api_version is not None and self.api_version.strip():
            json_dict["ApiVersion"] = self.api_version
        return json_dict
