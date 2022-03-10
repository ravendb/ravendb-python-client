from typing import List

from ravendb.documents.session.tokens.query_tokens.query_token import QueryToken
import ravendb.documents.session.tokens.query_tokens.definitions as tokens


class DocumentQueryHelper:
    @staticmethod
    def add_space_if_needed(previous_token: QueryToken, current_token: QueryToken, writer: List[str]) -> None:
        if previous_token is None:
            return

        if isinstance(previous_token, tokens.OpenSubclauseToken) or isinstance(
            current_token, (tokens.CloseSubclauseToken, tokens.IntersectMarkerToken)
        ):
            return

        writer.append(" ")
