from __future__ import annotations

import json
from typing import Any, Dict, Optional


class AiOutputOptions:
    """
    Overrides the output format for a single conversation turn.

    The agent still needs a default output schema when it is created; these options take
    precedence over it, and only for the turn they are passed to.

    Give it exactly one of:

    - ``sample_object`` - an object the server turns into a JSON schema at request time
    - ``output_schema`` - an explicit JSON schema string
    - ``no_schema=True`` - no structured output at all, so the model answers in free text
    """

    def __init__(
        self,
        sample_object: Any = None,
        output_schema: str = None,
        no_schema: bool = False,
    ):
        if no_schema and (sample_object is not None or output_schema is not None):
            raise ValueError(
                "no_schema asks the model for free-form text, so it cannot be combined with "
                "an output schema or a sample object. Drop one of them."
            )

        if output_schema is not None and (not output_schema or output_schema.isspace()):
            raise ValueError("output_schema cannot be empty or whitespace")

        self.sample_object = sample_object
        # Takes precedence over sample_object when both are set.
        self.output_schema = output_schema
        self.no_schema = no_schema

    def to_json(self) -> Dict[str, Any]:
        json_dict = {}

        if self.sample_object is not None:
            sample = self.sample_object
            if callable(getattr(sample, "to_json", None)):
                sample = sample.to_json()
            # The server reads this as a JSON string, not as a nested object.
            json_dict["SampleObject"] = json.dumps(sample)

        if self.output_schema is not None:
            json_dict["OutputSchema"] = self.output_schema

        if self.no_schema:
            json_dict["NoSchema"] = True

        return json_dict

    @classmethod
    def from_json(cls, json_dict: Optional[Dict[str, Any]]) -> Optional[AiOutputOptions]:
        if not json_dict:
            return None

        options = cls.__new__(cls)
        sample = json_dict.get("SampleObject")
        if sample is not None:
            options.sample_object = json.loads(sample) if isinstance(sample, str) else sample
        else:
            options.sample_object = None
        options.output_schema = json_dict.get("OutputSchema")
        options.no_schema = json_dict.get("NoSchema", False)
        return options
