from __future__ import annotations

from enum import Enum


class AiConversationDetailLevel(Enum):
    """
    Controls the level of detail when reading conversation messages.

    Simple: User messages (including attachment-only) and assistant messages
        that have content only. System prompts, tool calls, summaries, and
        internal messages are excluded.
    Detailed: Includes system messages, tool calls with results, and per-message
        usage. Summaries and internal messages are excluded.
    Full: No filtering — includes all messages: system, tool calls, summaries,
        internal. Intended for debugging and future-proofing.
    """

    SIMPLE = "Simple"
    DETAILED = "Detailed"
    FULL = "Full"
