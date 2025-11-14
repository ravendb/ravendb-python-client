from __future__ import annotations
from typing import Optional, TypeVar, Generic, TYPE_CHECKING
from datetime import timedelta
import enum

if TYPE_CHECKING:
    from ravendb.documents.operations.ai.agents import AiUsage

TAnswer = TypeVar("TAnswer")


class AiConversationStatus(enum.Enum):
    """
    Represents the status of an AI conversation.
    """

    DONE = "Done"
    ACTION_REQUIRED = "ActionRequired"

    def __str__(self):
        return self.value


class AiAnswer(Generic[TAnswer]):
    """
    Represents the answer from an AI conversation turn.
    
    This class contains the AI's response, the conversation status,
    token usage statistics, and timing information.
    """

    def __init__(
        self,
        answer: Optional[TAnswer] = None,
        status: AiConversationStatus = AiConversationStatus.DONE,
        usage: Optional[AiUsage] = None,
        elapsed: Optional[timedelta] = None,
    ):
        """
        Initialize an AiAnswer instance.

        Args:
            answer: The answer content produced by the AI
            status: The status of the conversation (Done or ActionRequired)
            usage: Token usage reported by the model
            elapsed: The total time elapsed to produce the answer
        """
        self.answer = answer
        self.status = status
        self.usage = usage
        self.elapsed = elapsed

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"AiAnswer(status={self.status.value}, "
            f"has_answer={self.answer is not None}, "
            f"elapsed={self.elapsed})"
        )

    def __repr__(self) -> str:
        """Detailed representation for debugging."""
        return (
            f"AiAnswer(answer={self.answer!r}, "
            f"status={self.status!r}, "
            f"usage={self.usage!r}, "
            f"elapsed={self.elapsed!r})"
        )

