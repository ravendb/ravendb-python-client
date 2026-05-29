from __future__ import annotations

from typing import Optional

from ravendb.documents.session.misc import MethodCall, CmpXchg
from ravendb.documents.session.tokens.query_tokens.definitions import WhereToken


class RavenDocumentQuery:
    """Server-side RQL functions for DocumentQuery: now(), today(), cmpxchg()."""

    @staticmethod
    def now(offset: Optional[str] = None) -> "RavenDocumentQuery.Time":
        if offset is None:
            return RavenDocumentQuery.Time(WhereToken.MethodsType.NOW)
        return RavenDocumentQuery.Time(WhereToken.MethodsType.NOW, [offset])

    @staticmethod
    def today() -> "RavenDocumentQuery.Time":
        return RavenDocumentQuery.Time(WhereToken.MethodsType.TODAY)

    @staticmethod
    def cmp_xchg(key: str) -> CmpXchg:
        # Build directly — CmpXchg.value() is deprecated and emits a warning.
        cmp_xchg = CmpXchg()
        cmp_xchg.args = [key]
        return cmp_xchg

    class Time(MethodCall):
        def __init__(self, method_type: WhereToken.MethodsType, args=None):
            super().__init__(args=args or [])
            self.method_type = method_type
