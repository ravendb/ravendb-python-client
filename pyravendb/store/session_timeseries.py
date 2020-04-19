from datetime import datetime
import sys


class TimeSeries:

    @staticmethod
    def throw_not_in_session(entity):
        raise ValueError(
            repr(entity) + " is not associated with the session, cannot add time-series to it. "
                           "Use document Id instead or track the entity in the session.")

    def __init__(self, session, entity_or_document_id, name):
        self._session = session
        if not isinstance(entity_or_document_id, str):
            entity = self._session.documents_by_entity.get(entity_or_document_id, None)
            if not entity:
                self.throw_not_in_session(entity_or_document_id)
            entity_or_document_id = entity["metadata"]["@id"]

        if not entity_or_document_id:
            raise ValueError(entity_or_document_id)
        if not name:
            raise ValueError(name)

    def get(self, from_date: datetime, to_date: datetime, start: int = 0, page_size: int = sys.maxsize):
        raise NotImplemented("Time-series from the session is not implemented")