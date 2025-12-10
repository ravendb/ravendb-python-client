from typing import Dict, Any, Tuple


class GenAiTransformation:
    """
    Represents the transformation script configuration for GenAI processing.
    The script must call ai.genContext(ctx) function.
    """

    def __init__(self, script: str = None):
        self.script = script

    def validate_script(self) -> Tuple[bool, str]:
        """
        Validates that the script contains the required ai.genContext call.

        Returns:
            A tuple of (is_valid, error_message). If valid, error_message is empty.
        """
        if self.script and "ai.genContext" in self.script:
            return True, ""
        return False, "You must call the ai.genContext(ctx) function in your script"

    def to_json(self) -> Dict[str, Any]:
        return {
            "Script": self.script,
        }

    @classmethod
    def from_json(cls, json_dict: Dict[str, Any]) -> "GenAiTransformation":
        return cls(
            script=json_dict.get("Script"),
        )
