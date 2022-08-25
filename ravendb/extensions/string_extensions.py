from typing import List

from ravendb.tools.utils import Utils


def to_web_socket_path(path: str) -> str:
    return path.replace("http://", "ws://").replace("https://", "wss://")


def is_identifier(token: str, start: int = 0, length=None) -> bool:
    if length == 0 or length > 256:
        return False
    if token[start].isalpha() and token[start] != "_":
        return False

    for i in range(1, length):
        if not token[start + i].isalnum() and token[start + i] != "_":
            return False

    return True


def escape_string(builder: List[str], value: str) -> None:
    if not value or value.isspace():
        return

    _escape_string_internal(builder, value)


def _escape_string_internal(builder: List[str], value: str) -> None:
    escaped = Utils.escape(value)  # todo: escape_ecma_script
    builder.append(escaped)
